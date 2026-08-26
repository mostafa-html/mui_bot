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
