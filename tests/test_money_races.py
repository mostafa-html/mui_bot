"""Regression tests for the money-race fixes:

1. panel_update_lock serializes per-service read-panel->write-panel sections
   so overlapping renew/topup runs can't clobber each other's paid increment.
2. Rejecting an invoice refunds the coupon usage booked at receipt time.
3. claim_reward flips reward_claimed atomically — a double tap grants once.
"""
import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from tests._bootstrap import bot as _b, seed_plan  # bootstrap first
import bot
import tasks
from database import (SessionLocal, Invoice, Plan, Coupon, CouponUsage,
                      ReferralCode, Referral, AppSetting)


# ------------------------------------------------------------- fake redis

class _FakeRedis:
    """Just enough of redis.asyncio for panel_update_lock: SET NX EX + Lua del."""

    store = {}

    def __init__(self):
        self._token = None

    @classmethod
    def reset(cls):
        cls.store = {}

    async def set(self, key, value, nx=False, ex=None):
        if nx and key in self.store:
            return False
        self.store[key] = value
        return True

    async def get(self, key):
        return self.store.get(key)

    async def eval(self, script, numkeys, key, token):
        # mirrors _LOCK_RELEASE_LUA: delete only if we still own the key
        if self.store.get(key) == token:
            del self.store[key]
            return 1
        return 0

    async def aclose(self):
        pass


@pytest.fixture
def fake_redis(monkeypatch):
    _FakeRedis.reset()
    monkeypatch.setattr(tasks.redis.Redis, "from_url", lambda *a, **k: _FakeRedis())
    yield _FakeRedis


# ------------------------------------------------------------------- fix 1

def test_lock_is_mutually_exclusive(fake_redis):
    """Second acquirer blocks until the first releases."""
    events = []

    async def worker(tag, hold):
        async with tasks.panel_update_lock("svc-1", wait=5):
            events.append(f"{tag}:in")
            await asyncio.sleep(hold)
            events.append(f"{tag}:out")

    async def main():
        await asyncio.gather(worker("a", 0.15), worker("b", 0.0))

    asyncio.run(main())
    # critical sections must never interleave
    assert events == ["a:in", "a:out", "b:in", "b:out"] or \
           events == ["b:in", "b:out", "a:in", "a:out"]


def test_lock_timeout_raises_for_retry(fake_redis):
    """A lock held past the wait budget must raise (Celery autoretries),
    NOT silently proceed into the critical section."""

    async def main():
        async with tasks.panel_update_lock("svc-2", ttl=60):
            with pytest.raises(RuntimeError):
                async with tasks.panel_update_lock("svc-2", wait=0.1):
                    pass  # pragma: no cover

    asyncio.run(main())


def test_lock_degrades_unlocked_when_redis_down(monkeypatch):
    """Redis unreachable → warn + proceed unlocked (pre-lock behaviour),
    never crash provisioning."""
    import redis as _redis

    class _Boom:
        @classmethod
        def from_url(cls, *a, **k):
            raise ConnectionError("no redis")

    monkeypatch.setattr(tasks.redis.Redis, "from_url", _Boom.from_url)

    async def main():
        async with tasks.panel_update_lock("svc-3"):
            pass

    asyncio.run(main())  # must not raise


def test_concurrent_renews_do_not_lose_increments(fake_redis):
    """Two overlapping provision_renew executions on one service must both
    apply their paid GB — serialized by the lock, no last-writer-wins loss."""
    now = datetime.now(timezone.utc)
    email = "race_renew_user_910"
    with SessionLocal() as db:
        db.query(Invoice).filter(Invoice.client_name.like(f"{email}%")).delete()
        plan = db.query(Plan).filter(Plan.id == 777).first()
        if not plan:
            plan = Plan(id=777, name="race-plan", traffic_gb=10, duration_days=30,
                        price=100000, is_active=True, service_type='xui')
            db.add(plan)
        db.add(Invoice(telegram_user_id=911, action_type="RENEW",
                       client_name=email, status="PENDING",
                       plan_id=plan.id, total_price=100000, created_at=now))
        db.commit()

    class _FakeXUI:
        """Stateful panel: get/update/reset with awaits to allow interleaving."""
        state = {"totalGB": 5 * 1024**3, "expiryTime": 9999999999999, "tgId": 0}
        updates = 0

        async def get_client_full(self, em):
            await asyncio.sleep(0.01)          # widen the race window
            return {"client": dict(self.state), "usedTraffic": 0}

        async def update_client(self, em, payload):
            await asyncio.sleep(0.01)
            type(self).updates += 1
            self.state.update(payload)

        async def reset_client_traffic(self, em):
            pass

    orig = (tasks.XUIClient, tasks.notify_user, tasks.notify_many_users,
            tasks.animate_loading_message, tasks.get_sub_link, tasks.mark_referral_paid,
            tasks.invalidate_cache)
    tasks.XUIClient = _FakeXUI
    tasks.notify_user = lambda *a, **k: _noop()
    tasks.notify_many_users = _noop_many
    tasks.animate_loading_message = _noop_many
    tasks.get_sub_link = _noop_str
    tasks.mark_referral_paid = lambda uid: []
    tasks.invalidate_cache = _noop_many
    try:
        inv_ids = []
        with SessionLocal() as db:
            for _ in range(2):   # two PENDING invoices, same service
                inv = Invoice(telegram_user_id=911, action_type="RENEW",
                              client_name=email, status="PENDING",
                              plan_id=777, total_price=100000, created_at=now)
                db.add(inv)
                db.commit()
                inv_ids.append(inv.id)

        async def once(iid):
            async with tasks.panel_update_lock(f"xui:{email}"):
                await tasks._renew_body(iid, email)

        async def main():
            await asyncio.gather(*(once(i) for i in inv_ids))

        asyncio.run(main())

        # both increments survived: 5 + 10 + 10 = 25 GB (lost update would be 15)
        assert _FakeXUI.updates == 2
        assert _FakeXUI.state["totalGB"] == 25 * 1024**3
    finally:
        (tasks.XUIClient, tasks.notify_user, tasks.notify_many_users,
         tasks.animate_loading_message, tasks.get_sub_link, tasks.mark_referral_paid,
         tasks.invalidate_cache) = orig


async def _noop(*a, **k):
    pass


async def _noop_many(*a, **k):
    pass


async def _noop_str(*a, **k):
    return ""


# ------------------------------------------------------------------- fix 2

def test_rejection_refunds_coupon_usage():
    """admin_reject_reason must delete CouponUsage rows for the invoice."""
    now = datetime.now(timezone.utc)
    with SessionLocal() as db:
        db.query(CouponUsage).filter(CouponUsage.user_id == 933).delete()
        db.query(Coupon).filter(Coupon.code == "RACETEST").delete()
        db.query(Invoice).filter(Invoice.telegram_user_id == 933).delete()
        db.commit()
        coupon = Coupon(id=901, code="RACETEST", discount_type="percent",
                        discount_value=10, max_uses_total=1, max_uses_per_user=1,
                        active=True)
        db.add(coupon)
        inv = Invoice(telegram_user_id=933, action_type="NEW", status="PENDING",
                      total_price=100000, coupon_code="RACETEST", created_at=now)
        db.add(inv)
        db.commit()
        db.add(CouponUsage(id=coupon.id, coupon_id=coupon.id, user_id=933,
                           invoice_id=inv.id))
        db.commit()
        invoice_id, coupon_id = inv.id, coupon.id

    sent = []
    orig_send = bot.bot.send_message
    bot.bot.send_message = lambda chat_id, text, **k: _capture(sent, text)

    class _Msg:
        from_user = SimpleNamespace(id=111)          # admin
        text = "رسید نامعتبر است"

        async def answer(self, *a, **k):
            pass

    class _State:
        data = {"invoice_id": invoice_id}

        async def get_data(self):
            return dict(self.data)

        async def clear(self):
            pass

    try:
        asyncio.run(bot.admin_reject_reason(_Msg(), _State()))
    finally:
        bot.bot.send_message = orig_send

    with SessionLocal() as db:
        assert db.query(Invoice).filter(Invoice.id == invoice_id).one().status == "REJECTED"
        # quota returned: the usage row is gone
        assert db.query(CouponUsage).filter(
            CouponUsage.invoice_id == invoice_id).count() == 0
        db.query(CouponUsage).filter(CouponUsage.coupon_id == coupon_id).delete()
        db.query(Coupon).filter(Coupon.id == coupon_id).delete()
        db.query(Invoice).filter(Invoice.id == invoice_id).delete()
        db.commit()


async def _capture(sent, text):
    sent.append(text)


# ------------------------------------------------------------------- fix 3

def _seed_referral_ready(user_id, threshold_met=True):
    now = datetime.now(timezone.utc)
    seed_plan(777, name='race-reward', gb=5, days=30, price=50000, service_type='xui')
    with SessionLocal() as db:
        db.query(ReferralCode).filter(ReferralCode.telegram_user_id == user_id).delete()
        db.query(Referral).filter(Referral.referrer_id == user_id).delete()
        db.commit()
        db.add(ReferralCode(id=user_id, telegram_user_id=user_id,
                            code=f"RACE{user_id}", reward_claimed=False))
        if threshold_met:
            thr = db.query(AppSetting).filter(AppSetting.key == "referral_threshold").first()
            saved = thr.value if thr else None
            if thr:
                thr.value = "1"
            else:
                db.add(AppSetting(key="referral_threshold", value="1"))
            db.add(Referral(id=user_id, referrer_id=user_id,
                            referred_user_id=user_id,
                            became_paid=True, paid_at=now, referred_at=now))
            reward_plan = db.query(AppSetting).filter(AppSetting.key == "referral_reward_plan_id").first()
            saved_reward_plan = reward_plan.value if reward_plan else None
            if reward_plan:
                reward_plan.value = "777"
            else:
                db.add(AppSetting(key="referral_reward_plan_id", value="777"))
                reward_plan = None
            db.commit()
            return (saved, saved_reward_plan)
        db.commit()
        return None


def test_claim_reward_double_tap_grants_once(fake_redis=None):
    user_id = 944
    saved_threshold, saved_reward_plan = _seed_referral_ready(user_id)

    dispatched = []

    class _FakeTask:
        def delay(self, *a, **k):
            dispatched.append(a)

    orig_task = tasks.provide_referral_reward
    tasks.provide_referral_reward = _FakeTask()
    try:
        answered = []

        def make_cb():
            cb = SimpleNamespace(from_user=SimpleNamespace(id=user_id))
            cb.message = SimpleNamespace(
                answer=lambda *a, **k: _capture(answered, *a))
            async def _answer(text=None, show_alert=False):
                answered.append(("alert", text))
            cb.answer = _answer
            return cb

        async def main():
            await asyncio.gather(bot.claim_reward(make_cb()),
                                 bot.claim_reward(make_cb()))

        asyncio.run(main())

        # exactly ONE of the two taps may dispatch the reward task
        assert len(dispatched) == 1
    finally:
        tasks.provide_referral_reward = orig_task
        with SessionLocal() as db:
            rec = db.query(ReferralCode).filter(
                ReferralCode.telegram_user_id == user_id).first()
            assert rec.reward_claimed is True
            # cleanup
            db.delete(rec)
            for r in db.query(Referral).filter(Referral.referrer_id == user_id):
                db.delete(r)
            for key, saved in (("referral_threshold", saved_threshold),
                               ("referral_reward_plan_id", saved_reward_plan)):
                row = db.query(AppSetting).filter(AppSetting.key == key).first()
                if saved is None and row:
                    db.delete(row)
                elif row and saved is not None:
                    row.value = saved
            db.commit()


def test_cleanup_race_tables():
    with SessionLocal() as db:
        db.query(Invoice).filter(Invoice.client_name.like("race_renew_user%")).delete()
        db.commit()


def test_provision_renew_task_wires_the_lock(fake_redis):
    """Full task path: provision_renew -> _run -> panel_update_lock -> body.
    Proves the real entrypoint serializes AND releases its lock cleanly."""
    seed_plan(888, name='race-e2e', gb=10, days=30, price=100000, service_type='xui')
    email = "race_wire_user_955"
    now = datetime.now(timezone.utc)
    with SessionLocal() as db:
        db.query(Invoice).filter(Invoice.client_name.like(f"{email}%")).delete()
        db.commit()
        inv = Invoice(telegram_user_id=955, action_type="RENEW",
                      client_name=email, status="PENDING",
                      plan_id=888, total_price=100000, created_at=now)
        db.add(inv)
        db.commit()
        inv_id = inv.id

    class _FakeXUI:
        state = {"totalGB": 5 * 1024**3, "expiryTime": 9999999999999, "tgId": 0}

        async def get_client_full(self, em):
            return {"client": dict(self.state), "usedTraffic": 0}

        async def update_client(self, em, payload):
            self.state.update(payload)

        async def reset_client_traffic(self, em):
            pass

    orig = (tasks.XUIClient, tasks.notify_user, tasks.notify_many_users,
            tasks.animate_loading_message, tasks.get_sub_link, tasks.mark_referral_paid,
            tasks.invalidate_cache)
    (tasks.XUIClient, tasks.notify_user, tasks.notify_many_users,
     tasks.animate_loading_message, tasks.get_sub_link, tasks.mark_referral_paid,
     tasks.invalidate_cache) = (_FakeXUI,) + (_noop, _noop_many, _noop_many,
                                              _noop_str, lambda uid: [], _noop_many)
    try:
        tasks.provision_renew(inv_id, email)   # real celery task entrypoint
    finally:
        (tasks.XUIClient, tasks.notify_user, tasks.notify_many_users,
         tasks.animate_loading_message, tasks.get_sub_link, tasks.mark_referral_paid,
         tasks.invalidate_cache) = orig

    with SessionLocal() as db:
        assert db.query(Invoice).filter(Invoice.id == inv_id).one().status == "COMPLETE"
    assert _FakeXUI.state["totalGB"] == 15 * 1024**3      # 5 base + 10 renewed
    assert fake_redis.store == {}, "lock was not released"
