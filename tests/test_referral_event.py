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
