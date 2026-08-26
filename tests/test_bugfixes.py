"""Regression tests for three production bugs:

1. tasks.add_reseller_pack referenced TrafficPack without importing it —
   every reseller pack purchase crashed with NameError (3 futile retries).
2. bot.admin_toggle_pack called admin_traffic_packs(callback, None) while
   the handler unconditionally ran state.clear() → AttributeError per tap.
3. Deletion sweep sent warnings ONLY on exact days 7/8/9 post-expiry but
   required warning_count >= 3 to delete: one missed daily run and the
   expired service was never cleaned up.
"""
import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from tests._bootstrap import bot as _b  # bootstrap first (pins env, imports app)
import bot  # noqa: E402  (safe to import after _bootstrap pinned env)
import tasks
from database import SessionLocal, Invoice, TrafficPack, ResellerPack


DAY = timedelta(days=1)
HOUR_MS = 3600 * 1000


def _swap_reseller_packs_table():
    """SQLite cannot autoincrement BigInteger PKs (PostgreSQL BIGSERIAL is
    unaffected) — give reseller_packs an INTEGER PK so task inserts work."""
    from database import engine
    from sqlalchemy import text
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS reseller_packs"))
        conn.execute(text("""
            CREATE TABLE reseller_packs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                reseller_id BIGINT NOT NULL,
                granted_bytes BIGINT NOT NULL,
                used_bytes BIGINT NOT NULL DEFAULT 0,
                expiry_date DATETIME NOT NULL,
                created_at DATETIME,
                is_active BOOLEAN DEFAULT 1 NOT NULL
            )"""))


def _clean(pfx):
    with SessionLocal() as db:
        db.query(Invoice).filter(Invoice.client_name.like(f"{pfx}%")).delete()
        db.query(ResellerPack).filter(ResellerPack.id == 4242).delete()
        db.query(TrafficPack).filter(TrafficPack.name.like(f"{pfx}%")).delete()
        db.commit()


# ---------------------------------------------------------------- fix #1

def test_add_reseller_pack_no_nameerror():
    """PENDING invoice must cross the TrafficPack query without NameError
    and end up COMPLETE with a ResellerPack row granted."""
    _clean("bugfix-pack")
    _swap_reseller_packs_table()
    pfx = "bugfix-pack"
    now = datetime.now(timezone.utc)
    with SessionLocal() as db:
        # BigInteger PKs can't autoincrement on SQLite — set explicit ids
        pack = TrafficPack(id=4242, name=f"{pfx} 10GB", traffic_gb=10,
                           duration_days=30, price=100000, is_active=True)
        db.add(pack)
        db.flush()
        inv = Invoice(telegram_user_id=4242, action_type="RESELLER_PACK",
                      total_price=100000, status="PENDING", pack_id=pack.id,
                      reseller_id=4242, created_at=now)
        db.add(inv)
        db.commit()
        invoice_id = inv.id

    orig_notify = tasks.notify_user
    sent = []
    tasks.notify_user = lambda tg_id, text, effect=None: _capture(sent, tg_id, text)
    try:
        tasks.add_reseller_pack(invoice_id)  # raised NameError before the fix
    finally:
        tasks.notify_user = orig_notify

    with SessionLocal() as db:
        inv = db.query(Invoice).filter(Invoice.id == invoice_id).first()
        assert inv.status == "COMPLETE"
        rp = db.query(ResellerPack).filter(ResellerPack.reseller_id == 4242).first()
        assert rp is not None and rp.granted_bytes == 10 * 1024 ** 3
    assert any("خریداری شد" in t for _, t in sent)


async def _capture(sent, tg_id, text):
    sent.append((tg_id, text))


def test_add_reseller_pack_missing_pack_fails_gracefully():
    """PENDING invoice with a dangling pack_id → FAILED status, no crash."""
    now = datetime.now(timezone.utc)
    with SessionLocal() as db:
        inv = Invoice(telegram_user_id=4242, action_type="RESELLER_PACK",
                      total_price=100000, status="PENDING", pack_id=999999,
                      reseller_id=4242, created_at=now)
        db.add(inv)
        db.commit()
        invoice_id = inv.id

    orig_notify = tasks.notify_user
    sent = []
    tasks.notify_user = lambda tg_id, text, effect=None: _capture(sent, tg_id, text)
    try:
        tasks.add_reseller_pack(invoice_id)
    finally:
        tasks.notify_user = orig_notify

    with SessionLocal() as db:
        inv = db.query(Invoice).filter(Invoice.id == invoice_id).first()
        assert inv.status == "FAILED"
    assert any("یافت نشد" in t for _, t in sent)


# ---------------------------------------------------------------- fix #2

def test_admin_traffic_packs_accepts_none_state():
    """The refresh call from admin_toggle_pack passes state=None; the
    handler must render normally instead of crashing on state.clear()."""
    _clean("bugfix-view")
    with SessionLocal() as db:
        db.add(TrafficPack(id=4243, name="bugfix-view GB", traffic_gb=5,
                           duration_days=30, price=50000, is_active=True))
        db.commit()

    cb = SimpleNamespace(from_user=SimpleNamespace(id=111))  # admin per bootstrap
    drawn = []

    class _Msg:
        async def edit_text(self, text=None, *a, **k):
            drawn.append(text)

        async def answer(self, *a, **k):
            pass
    cb.message = _Msg()

    asyncio.run(bot.admin_traffic_packs(cb, None))  # AttributeError before the fix
    assert any("مدیریت بسته‌های ترافیک" in t for t in drawn)


# ---------------------------------------------------------------- fix #3

class _FakeXUI:
    """Stands in for XUIClient: expiry lookup by email + delete tracking."""

    expiry_by_email = {}
    deleted = []

    def __init__(self):
        pass

    async def get_client_full(self, email):
        exp = self.expiry_by_email.get(email)
        if exp is None:
            return None
        return {"client": {"expiryTime": exp}}

    async def delete_client(self, email):
        self.deleted.append(email)


def _seed_expired_invoice(email, expiry_dt, scheduled_at, warnings):
    now = datetime.now(timezone.utc)
    with SessionLocal() as db:
        inv = Invoice(telegram_user_id=777, action_type="NEW",
                      client_name=email, status="COMPLETE",
                      deletion_scheduled_at=scheduled_at,
                      deletion_warning_sent_count=warnings,
                      created_at=now - DAY * 40)
        db.add(inv)
        db.commit()
        return inv.id


def _run_sweep():
    orig_xui, orig_notify = tasks.XUIClient, tasks.notify_many_users
    notes = []
    async def _notes(items):
        notes.extend((i[0], i[1]) for i in items)
    tasks.XUIClient = _FakeXUI
    tasks.notify_many_users = _notes
    try:
        tasks.check_expired_services_for_deletion()
    finally:
        tasks.XUIClient, tasks.notify_many_users = orig_xui, orig_notify
    return notes


def _state(invoice_id):
    with SessionLocal() as db:
        inv = db.query(Invoice).filter(Invoice.id == invoice_id).first()
        return inv.status, inv.deletion_warning_sent_count


@pytest.fixture(autouse=True)
def _reset_fake():
    _FakeXUI.expiry_by_email = {}
    _FakeXUI.deleted = []
    yield


def test_deletion_normal_path_3_warnings():
    """Expired 11d ago, all 3 warnings delivered → deleted on schedule."""
    now = datetime.now(timezone.utc)
    email = "bugfix-del-normal_user_9001"
    expiry = now - timedelta(days=11)
    iid = _seed_expired_invoice(email, expiry, expiry + timedelta(days=10), 3)
    _FakeXUI.expiry_by_email[email] = (expiry.timestamp() * 1000) + HOUR_MS * 0.5
    _run_sweep()
    status, count = _state(iid)
    assert status == "DELETED" and email in _FakeXUI.deleted


def test_deletion_catchup_warning_after_missed_sweep():
    """REGRESSION for the deadlock: expired 12d ago but zero warnings were
    ever sent (daily runs missed during days 7-9). Old code could neither
    warn again (day not in [7,8,9]) nor delete (count<3) — stuck forever.
    New code sends a catch-up warning now, deletes at day >= 14."""
    now = datetime.now(timezone.utc)
    email = "bugfix-del-catchup_user_9002"
    expiry = now - timedelta(days=12)
    iid = _seed_expired_invoice(email, expiry, expiry + timedelta(days=10), 0)
    _FakeXUI.expiry_by_email[email] = (expiry.timestamp() * 1000) + HOUR_MS * 0.5
    notes = _run_sweep()
    status, count = _state(iid)
    # not past the hard cap yet → kept, but the missed warning catches up
    assert status == "COMPLETE" and count == 1
    assert any(uid == 777 and "هشدار حذف سرویس" in t for uid, t in notes)

    # two more sweeps fill the quota, then the service is deletable
    _run_sweep()
    _run_sweep()
    status, count = _state(iid)
    assert status == "DELETED" and count == 3


def test_deletion_hard_cap_after_prolonged_outage():
    """Expired 15d ago, worker was down the whole window → count still 0,
    yet cleanup MUST happen instead of lingering forever."""
    now = datetime.now(timezone.utc)
    email = "bugfix-del-outage_user_9003"
    expiry = now - timedelta(days=15)
    iid = _seed_expired_invoice(email, expiry, expiry + timedelta(days=10), 0)
    _FakeXUI.expiry_by_email[email] = (expiry.timestamp() * 1000) + HOUR_MS * 0.5
    _run_sweep()
    status, count = _state(iid)
    assert status == "DELETED" and email in _FakeXUI.deleted


def test_deletion_not_due_yet_still_warns():
    """Expired 8d ago, inside the warning window → warned, kept alive."""
    now = datetime.now(timezone.utc)
    email = "bugfix-del-window_user_9004"
    expiry = now - timedelta(days=8)
    iid = _seed_expired_invoice(email, expiry, expiry + timedelta(days=10), 0)
    _FakeXUI.expiry_by_email[email] = (expiry.timestamp() * 1000) + HOUR_MS * 0.5
    notes = _run_sweep()
    status, count = _state(iid)
    assert status == "COMPLETE" and count == 1
    assert any("هشدار حذف سرویس" in t for _, t in notes)


def test_cleanup():
    _clean("bugfix-")
