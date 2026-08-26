"""Referral Events: models, credit engine, reward provisioning, UI helpers.
Offline suite — panel/broker/Telegram boundaries are faked."""
from datetime import datetime, timedelta, timezone

from tests._bootstrap import database  # noqa: F401  pins env + schema first


def test_event_models_exist_with_expected_columns():
    from database import ReferralEvent, ReferralEventReward, Referral
    cols = {c.name for c in ReferralEvent.__table__.columns}
    assert {'id', 'title', 'required_invites', 'service_type', 'vless_plan_id',
            'amnezia_gb', 'amnezia_days', 'starts_at', 'ends_at',
            'is_active'} <= cols
    rcols = {c.name for c in ReferralEventReward.__table__.columns}
    assert {'id', 'event_id', 'referrer_id', 'granted_at', 'service_type'} <= rcols
    assert 'paid_at' in {c.name for c in Referral.__table__.columns}


def test_paid_at_column_present_in_actual_sqlite_schema():
    from sqlalchemy import inspect
    from database import engine
    names = [c['name'] for c in inspect(engine).get_columns('referrals')]
    assert 'paid_at' in names


def test_init_db_is_idempotent_on_paid_at():
    import database
    database.init_db()   # second run must not raise duplicate-column
    from sqlalchemy import inspect
    names = [c['name'] for c in inspect(database.engine).get_columns('referrals')]
    assert 'paid_at' in names


def test_new_table_pks_autoincrement_on_sqlite():
    from database import SessionLocal, ReferralEvent, ReferralEventReward
    now = datetime.now(timezone.utc)
    with SessionLocal() as db:
        ev = ReferralEvent(title='t', required_invites=1, service_type='vless',
                           starts_at=now, ends_at=now + timedelta(hours=1))
        db.add(ev)
        db.flush()
        assert ev.id is not None
        rw = ReferralEventReward(event_id=ev.id, referrer_id=1,
                                 granted_at=now, service_type='vless')
        db.add(rw)
        db.flush()
        assert rw.id is not None
        db.delete(rw)
        db.delete(ev)
        db.commit()


HOUR = timedelta(hours=1)


def _clean_referral_tables():
    from database import engine
    from sqlalchemy import text
    with engine.begin() as conn:
        for t in ('referral_event_rewards', 'referral_events',
                  'referrals', 'referral_codes'):
            conn.execute(text(f"DELETE FROM {t}"))


def _seed_code(tg_id):
    from database import SessionLocal, ReferralCode
    with SessionLocal() as db:
        db.add(ReferralCode(id=tg_id, telegram_user_id=tg_id, code=f'ref_{tg_id}_aa'))
        db.commit()


def _seed_referral(referred, referrer, referred_at, became_paid=False, paid_at=None):
    from database import SessionLocal, Referral
    with SessionLocal() as db:
        db.add(Referral(id=referred, referrer_id=referrer, referred_user_id=referred,
                        referred_at=referred_at, became_paid=became_paid, paid_at=paid_at))
        db.commit()


def _seed_event(ev_id=1, x=2, start=None, hours=48, active=True, service='vless', plan_id=5):
    from database import SessionLocal, ReferralEvent
    now = datetime.now(timezone.utc)
    start = start or now - HOUR
    with SessionLocal() as db:
        db.add(ReferralEvent(id=ev_id, title='رویداد معرفی', required_invites=x,
                             service_type=service, vless_plan_id=plan_id if service == 'vless' else None,
                             amnezia_gb=5 if service == 'amnezia' else None,
                             amnezia_days=10 if service == 'amnezia' else None,
                             starts_at=start, ends_at=start + timedelta(hours=hours),
                             is_active=active))
        db.commit()


class _DelayStub:
    def __init__(self):
        self.calls = []
        self.orig = None

    def __enter__(self):
        import tasks
        self.orig = tasks.provide_event_reward.delay
        tasks.provide_event_reward.delay = lambda *a: self.calls.append(a)
        return self.calls

    def __exit__(self, *exc):
        import tasks
        tasks.provide_event_reward.delay = self.orig


def test_mark_referral_paid_sets_paid_at_once():
    _clean_referral_tables(); _seed_code(1001); _seed_code(2002)
    _seed_referral(2002, 1001, datetime.now(timezone.utc) - HOUR)
    from database import SessionLocal, Referral
    import tasks
    first = tasks.mark_referral_paid(2002)
    assert isinstance(first, list) and first and first[0][0] == 1001
    with SessionLocal() as db:
        r = db.query(Referral).filter(Referral.referred_user_id == 2002).first()
        assert r.became_paid is True and r.paid_at is not None
    second = tasks.mark_referral_paid(2002)          # idempotent flip
    assert second == []


def test_signup_before_window_never_counts():
    _clean_referral_tables(); _seed_code(1001); _seed_code(2003)
    now = datetime.now(timezone.utc)
    _seed_event(start=now - HOUR)                     # X=2 → needs 2 in-window buyers
    _seed_referral(2003, 1001, now - timedelta(days=10))   # signed up long ago
    with _DelayStub() as calls:
        import tasks
        tasks.mark_referral_paid(2003)
    from database import SessionLocal, ReferralEventReward
    with SessionLocal() as db:
        assert db.query(ReferralEventReward).count() == 0
    assert calls == []


def test_multi_reward_math_and_grant_once():
    _clean_referral_tables(); _seed_code(1001)
    now = datetime.now(timezone.utc)
    _seed_event(x=2, start=now - HOUR)
    _seed_referral(3001, 1001, now - timedelta(minutes=30))   # in-window signup
    _seed_referral(3002, 1001, now - timedelta(minutes=30))
    with _DelayStub() as calls:
        import tasks
        tasks.mark_referral_paid(3001)                 # count=1 → no grant yet
        assert calls == []
        tasks.mark_referral_paid(3002)                 # count=2 → exactly 1 grant
        assert len(calls) == 1 and calls[0] == (1001, 1)
        tasks.mark_referral_paid(3002)                 # replay → nothing new
        assert len(calls) == 1
    from database import SessionLocal, ReferralEventReward
    with SessionLocal() as db:
        rows = db.query(ReferralEventReward).all()
        assert len(rows) == 1 and rows[0].referrer_id == 1001 and rows[0].event_id == 1


def test_two_x_invites_yield_two_rewards():
    _clean_referral_tables(); _seed_code(1001)
    now = datetime.now(timezone.utc)
    _seed_event(x=1, start=now - HOUR)
    for uid in (4001, 4002):
        _seed_referral(uid, 1001, now - timedelta(minutes=30))
    with _DelayStub() as calls:
        import tasks
        for uid in (4001, 4002):
            tasks.mark_referral_paid(uid)
    assert len(calls) == 2


def test_ended_event_is_a_noop():
    _clean_referral_tables(); _seed_code(1001); _seed_code(5001)
    now = datetime.now(timezone.utc)
    _seed_event(active=False, start=now - HOUR)
    _seed_referral(5001, 1001, now - HOUR * 2)
    with _DelayStub() as calls:
        import tasks
        tasks.mark_referral_paid(5001)
    from database import SessionLocal, ReferralEventReward
    with SessionLocal() as db:
        assert db.query(ReferralEventReward).count() == 0
    assert calls == []


def test_get_active_event_boundaries():
    _clean_referral_tables()
    now = datetime.now(timezone.utc)
    from database import SessionLocal
    from tasks import get_active_event
    # future event → not yet active
    _seed_event(ev_id=9, start=now + HOUR, hours=48)
    with SessionLocal() as db:
        assert get_active_event(db) is None
    # live event (started 1h ago, 48h long) → found
    _clean_referral_tables()
    _seed_event(ev_id=10, start=now - HOUR, hours=48)
    with SessionLocal() as db:
        ev = get_active_event(db)
        assert ev is not None and ev.id == 10
    # expired 1 minute ago → inactive
    _clean_referral_tables()
    _seed_event(ev_id=11, start=now - timedelta(hours=49), hours=48)
    with SessionLocal() as db:
        assert get_active_event(db) is None


def _swap_services_table():
    """SQLite can't autoincrement BigInteger PKs — mirror test_amnezia_trial."""
    from database import engine
    from sqlalchemy import text
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS amnezia_services"))
        conn.execute(text("""
            CREATE TABLE amnezia_services (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                telegram_user_id BIGINT NOT NULL,
                connection_id VARCHAR(64) NOT NULL,
                client_id VARCHAR(255) NOT NULL,
                server_id BIGINT NOT NULL,
                server_name VARCHAR(100),
                protocol VARCHAR(20) NOT NULL,
                name VARCHAR(100) NOT NULL,
                quota_bytes BIGINT NOT NULL,
                expiry_date DATETIME NOT NULL,
                status VARCHAR(20),
                is_trial BOOLEAN DEFAULT 0 NOT NULL,
                invoice_id BIGINT,
                panel_user_id VARCHAR(64),
                panel_username VARCHAR(100),
                panel_password VARCHAR(128),
                expiry_warned_at DATETIME,
                server_missing_notified_at DATETIME,
                created_at DATETIME,
                updated_at DATETIME
            )"""))


class _PatchIO:
    """Silence + record every outbound side effect of provide_event_reward."""
    def __init__(self):
        self.notified, self.docs, self.calls = [], [], {}

    def __enter__(self):
        import tasks
        self._orig = {}
        for name in ('invalidate_cache', 'notify_user', 'send_user_document'):
            self._orig[name] = getattr(tasks, name)

        async def _noop_cache(tg_id):
            self.calls.setdefault('cache', []).append(tg_id)

        async def _rec_notify(tg_id, text, *a, **k):
            self.notified.append((tg_id, text))

        async def _rec_doc(tg_id, filename, content, caption):
            self.docs.append((tg_id, filename))

        tasks.invalidate_cache = _noop_cache
        tasks.notify_user = _rec_notify
        tasks.send_user_document = _rec_doc
        return self

    def __exit__(self, *exc):
        import tasks
        for name, val in self._orig.items():
            setattr(tasks, name, val)


class _FakeXUI:
    def __init__(self):
        pass

    async def get_enabled_inbounds(self):
        return [{'id': 1}, {'id': 2}]

    async def add_client(self, email, total_bytes, expiry_ms, inbound_ids):
        self.email, self.bytes, self.inbounds = email, total_bytes, inbound_ids

    async def assign_group(self, email, tg_id):
        self.group = (email, tg_id)


def test_vless_reward_provisions_from_bound_plan():
    _clean_referral_tables()
    from tests._bootstrap import seed_plan
    seed_plan(5, name='Evt 8GB', gb=8, days=25, price=1, service_type='xui')
    _seed_event(ev_id=3, service='vless', plan_id=5)
    import tasks
    fake = _FakeXUI()
    orig_xui = tasks.XUIClient
    tasks.XUIClient = lambda: fake
    try:
        with _PatchIO() as io:
            tasks.provide_event_reward.run(7001, 3)      # eager, no broker
    finally:
        tasks.XUIClient = orig_xui
    from database import SessionLocal, Invoice
    with SessionLocal() as db:
        inv = db.query(Invoice).filter(
            Invoice.action_type == 'EVENT_REWARD').order_by(Invoice.id.desc()).first()
        assert inv is not None
        assert inv.total_price == 0 and inv.plan_id == 5 and inv.telegram_user_id == 7001
    assert fake.email.startswith('event_7001_')
    assert fake.bytes == 8 * 1024 ** 3 and sorted(fake.inbounds) == [1, 2]
    assert any('رویداد' in t for _, t in io.notified)


def test_amnezia_reward_flags_trial_without_entitlement():
    _swap_services_table()
    _clean_referral_tables()
    _seed_event(ev_id=4, service='amnezia')
    import tasks

    class _FakeAmz:
        protocol = 'awg2'

        async def ensure_service_account(self, user_id, base_username, invoice_id):
            return {'panel_user_id': f'pu{user_id}', 'username': base_username,
                    'password': 'pw'}

        async def list_servers(self):
            return [{'id': 77, 'alive': False}, {'id': 88, 'alive': True}]

        async def add_connection(self, panel_user_id, server_id, name):
            return {'connection_id': 'c1', 'client_id': 'cl1', 'server_id': server_id,
                    'server_name': 'srv88', 'protocol': 'awg2'}

        async def update_user_limits(self, panel_user_id, user_id, quota_gb, expiry):
            self.quota_gb, self.expiry = quota_gb, expiry

        async def get_connection_config(self, server_id, client_id):
            return {'vpn_link': 'vless://fake', 'config': 'CONFTEXT'}

        async def close(self):
            pass

    fake = _FakeAmz()
    orig_amz = tasks.AmneziaClient
    tasks.AmneziaClient = lambda: fake
    try:
        with _PatchIO() as io:
            tasks.provide_event_reward.run(8001, 4)
    finally:
        tasks.AmneziaClient = orig_amz

    from database import SessionLocal, AmneziaService, AmneziaTrial, Invoice
    with SessionLocal() as db:
        svc = db.query(AmneziaService).order_by(AmneziaService.id.desc()).first()
        assert svc is not None and svc.is_trial is True and svc.server_id == 88
        assert svc.quota_bytes == 5 * 1024 ** 3          # ceil(5)=5 whole GB
        assert db.query(AmneziaTrial).filter(               # entitlement untouched
            AmneziaTrial.telegram_user_id == 8001).first() is None
        inv = db.query(Invoice).filter(
            Invoice.amnezia_service_id == svc.id).first()
        assert inv is not None and inv.action_type == 'EVENT_REWARD'
        assert inv.total_price == 0
    assert fake.quota_gb == 5
    assert any(f[1].endswith('.conf') for f in io.docs)
    assert 8001 in io.calls.get('cache', [])   # post-provision cache invalidation ran


def test_admin_event_card_builder():
    import bot
    now = datetime.now(timezone.utc)
    from database import ReferralEvent
    ev = ReferralEvent(id=1, title='رویداد معرفی', required_invites=3,
                       service_type='vless', vless_plan_id=5,
                       starts_at=now - HOUR, ends_at=now + timedelta(hours=47))
    card = bot.build_admin_event_card(ev, participants=7, rewards=2)
    assert 'رویداد معرفی' in card and '3' in card and '47' in card
    assert '7' in card and '2' in card


class _FakeCb:
    """Callback stand-in with admin identity (ADMIN_CHAT_IDS=111 in bootstrap)."""
    def __init__(self):
        from types import SimpleNamespace
        self.from_user = SimpleNamespace(id=111)

        class _Msg:
            async def edit_text(self, *a, **k):
                pass
            async def answer(self, *a, **k):
                pass
        self.message = _Msg()


async def test_admin_confirm_blocked_when_event_active():
    _clean_referral_tables()
    now = datetime.now(timezone.utc)
    _seed_event(ev_id=20, start=now - HOUR)           # an event is already live
    import asyncio
    from tests._bootstrap import FakeState
    from database import SessionLocal, ReferralEvent
    state = FakeState({'ev_goal': 2, 'ev_service': 'vless', 'ev_plan_id': 5,
                       'ev_hours': 48})
    import bot
    cb = _FakeCb()
    await asyncio.wait_for(bot.admin_event_confirm(cb, state), timeout=10)
    with SessionLocal() as db:
        # only the pre-existing event (id=20); no duplicate created
        assert db.query(ReferralEvent).count() == 1


def test_event_banner_builder():
    import bot
    now = datetime.now(timezone.utc)
    from database import ReferralEvent
    ev = ReferralEvent(title='رویداد معرفی', required_invites=3, service_type='amnezia',
                       amnezia_gb=5, amnezia_days=10,
                       starts_at=now - HOUR, ends_at=now + timedelta(hours=47))
    banner = bot.build_event_banner(ev, ev_count=4)
    assert 'رویداد معرفی' in banner
    assert '3' in banner and '4' in banner          # goal + progress toward next (6)
    assert '6' in banner                            # next multiple shown
    assert '47' in banner                           # countdown hours


def test_event_announcement_builder():
    import bot
    now = datetime.now(timezone.utc)
    from database import ReferralEvent
    ev = ReferralEvent(title='رویداد معرفی', required_invites=3, service_type='vless',
                       vless_plan_id=5,
                       starts_at=now - HOUR, ends_at=now + timedelta(hours=48))
    text = bot.build_event_announcement(ev, 'کانفیگ VLESS (پلن Evt 8GB)')
    assert isinstance(text, str)
    assert 'رویداد معرفی' in text
    assert 'کانفیگ VLESS (پلن Evt 8GB)' in text          # prize verbatim
    assert '۳' in text                                    # goal in Persian digits
    assert '۴۸' in text                                   # hours in Persian digits
    assert '3' not in text                                # no Latin goal digits leaked
    for marker in ('قوانین', 'تست رایگان', 'سقف نداریم', 'مهلت'):
        assert marker in text


def test_fa_digits_conversion():
    from src.utils.formatting import fa_digits
    assert fa_digits(48) == '۴۸'
    assert fa_digits('12') == '۱۲'
    assert fa_digits('a1b2') == 'a۱b۲'


def test_event_prize_line_pulls_admin_choices():
    import bot
    from tests._bootstrap import seed_plan
    seed_plan(9, name='Evt 15GB', gb=15, days=30, price=1, service_type='xui')
    _clean_referral_tables()
    now = datetime.now(timezone.utc)
    from database import SessionLocal, ReferralEvent
    ev = ReferralEvent(title='t', required_invites=2, service_type='vless',
                       vless_plan_id=9, starts_at=now, ends_at=now + HOUR)
    with SessionLocal() as db:
        db.add(ev)
        db.commit()
        line = bot._event_prize_line(db, ev)
        assert 'Evt 15GB' in line and '۱۵' in line and 'گیگابایت' in line and '۳۰ روزه' in line
        # amnezia path falls back to describe_event_reward (GB+days already concrete)
        ev.service_type = 'amnezia'
        ev.amnezia_gb = 5
        ev.amnezia_days = 10
        line2 = bot._event_prize_line(db, ev)
        assert 'Amnezia' in line2 and '۵ گیگابایت ۱۰ روزه' in line2


def test_event_report_builder_and_stats():
    import tasks
    _clean_referral_tables(); _seed_code(1001); _seed_code(1002)
    now = datetime.now(timezone.utc)
    _seed_event(ev_id=30, x=2, start=now - timedelta(hours=49), hours=48)
    _seed_referral(6001, 1001, now - timedelta(hours=48))
    _seed_referral(6002, 1001, now - timedelta(hours=47), became_paid=True, paid_at=now - timedelta(hours=46))
    _seed_referral(6003, 1001, now - timedelta(hours=47), became_paid=True, paid_at=now - timedelta(hours=45))
    _seed_referral(6010, 1001, now - timedelta(hours=20))
    _seed_referral(6004, 1002, now - timedelta(hours=40), became_paid=True, paid_at=now - timedelta(hours=39))
    from database import SessionLocal, ReferralEvent, ReferralEventReward
    with SessionLocal() as db:
        db.add(ReferralEventReward(event_id=30, referrer_id=1001,
                                   granted_at=now - timedelta(hours=44), service_type='vless'))
        db.commit()
    with SessionLocal() as db:
        ev = db.query(ReferralEvent).filter(ReferralEvent.id == 30).first()
        stats = tasks.event_report_stats(db, ev)
        assert stats['participants'] == 2
        assert stats['invites'] == 3                      # paid-only count
        assert stats['rewards'] == 1
        assert stats['top'][0] == (1001, 2, 1)            # rid, paid invites, rewards
        assert stats['top'][1] == (1002, 1, 0)
        text = tasks.build_event_report(db, ev, stats)
    # don't leak an expired-active event into the auto-report sweep test below
    with SessionLocal() as db:
        ev = db.query(ReferralEvent).filter(ReferralEvent.id == 30).first()
        ev.is_active = False
        ev.result_reported = True
        db.commit()
    assert 'برترین' in text and 'گزارش' in text
    assert str(1001) in text                              # IDs stay Latin (copyable)
    assert '۳' in text                                    # totals in Persian digits


def test_finished_event_auto_report_exactly_once():
    _clean_referral_tables()
    now = datetime.now(timezone.utc)
    _seed_event(ev_id=31, start=now - timedelta(hours=50), hours=48)
    import tasks
    from database import SessionLocal, ReferralEvent
    sent = []
    orig = tasks.notify_many_users

    async def _rec(notifs):
        sent.extend(notifs)

    tasks.notify_many_users = _rec
    try:
        tasks.report_finished_events.run()
    finally:
        tasks.notify_many_users = orig
    with SessionLocal() as db:
        ev = db.query(ReferralEvent).filter(ReferralEvent.id == 31).first()
        assert ev.is_active is False and ev.result_reported is True
    assert len(sent) == 1 and 'گزارش رویداد' in sent[0][1]
    tasks.notify_many_users = _rec
    try:
        tasks.report_finished_events.run()                # replay → silent
    finally:
        tasks.notify_many_users = orig
    assert len(sent) == 1                                 # exactly-once held


def test_auto_amz_base_prefers_name_then_username():
    import bot
    from types import SimpleNamespace as NS
    # English display name wins
    u = NS(first_name='Ali', last_name='Ahmadi', username='ali_gh', id=555)
    assert bot._auto_amz_base(u) == 'Ali_Ahmadi'
    # unusable (non-Latin) name falls back to @username
    u = NS(first_name='علی', last_name='', username='ali_gh', id=555)
    assert bot._auto_amz_base(u) == 'ali_gh'
    # neither present → None (provisioning falls back to tg_<id>)
    u = NS(first_name='', last_name=None, username=None, id=555)
    assert bot._auto_amz_base(u) is None
    # too-short name falls through to username
    u = NS(first_name='A', last_name='', username='bob', id=555)
    assert bot._auto_amz_base(u) == 'bob'
    # junk characters collapsed, trimmed of underscores
    u = NS(first_name='M@hdi!!', last_name='', username='fallback9', id=1)
    assert bot._auto_amz_base(u) == 'M_hdi'
    # long names truncated to panel-safe 24 chars
    u = NS(first_name='a' * 40, last_name='', username='x', id=1)
    assert bot._auto_amz_base(u) == 'a' * 24
