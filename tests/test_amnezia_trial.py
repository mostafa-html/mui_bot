"""Amnezia trial unit tests: entitlement reset, expiry-sweep deletion of the
whole trial panel account, and admin-menu wiring. All offline — panel calls
are faked at the tasks.AmneziaClient boundary."""
from datetime import datetime, timedelta, timezone

from tests._bootstrap import bot  # noqa: F401


def _swap_services_table():
    """SQLite cannot autoincrement BigInteger PKs; give amnezia_services an
    INTEGER PK locally (PostgreSQL is unaffected)."""
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


def _clean_state():
    """Full-suite safety: each trial test starts from empty trial tables
    (earlier tests in the same process leave rows behind)."""
    _swap_services_table()
    from database import engine
    from sqlalchemy import text
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM amnezia_trials"))


def _seed_trial(tg_id, expired=False):
    from database import SessionLocal, AmneziaService, AmneziaTrial
    now = datetime.now(timezone.utc)
    with SessionLocal() as db:
        svc = AmneziaService(
            telegram_user_id=tg_id,
            connection_id=f'conn-{tg_id}',
            client_id=f'client-{tg_id}',
            server_id=0,
            server_name='turkey-sub',
            protocol='awg2',
            name=f'trial_{tg_id}',
            quota_bytes=1024 ** 3,
            expiry_date=now - timedelta(days=1) if expired else now + timedelta(days=1),
            status='active',
            is_trial=True,
            panel_user_id=f'panel-{tg_id}',
            panel_username=f'trial{tg_id}',
        )
        db.add(svc)
        db.flush()
        db.add(AmneziaTrial(telegram_user_id=tg_id, service_id=svc.id))
        db.commit()
        return svc.id


def test_reset_amnezia_trials_clears_entitlements():
    from tests._bootstrap import bot as _b  # ensure env first
    _clean_state()
    from database import SessionLocal, AmneziaTrial
    import bot
    _seed_trial(777001)
    _seed_trial(777002)
    with SessionLocal() as db:
        assert db.query(AmneziaTrial).count() == 2
        n = bot._reset_amnezia_trials(db)
        db.commit()
        assert n == 2
        assert db.query(AmneziaTrial).count() == 0
    # service rows untouched by reset (sweep owns their lifecycle)
    with SessionLocal() as db:
        from database import AmneziaService
        assert db.query(AmneziaService).filter(
            AmneziaService.is_trial == True).count() == 2


def test_expiry_sweep_deletes_expired_trial_panel_account():
    import asyncio
    from tests._bootstrap import bot as _b
    _clean_state()
    from database import SessionLocal, AmneziaService
    import tasks

    deleted = []
    enabled = []

    class FakeClient:
        async def get_user_stats(self, username):
            return {'expiration_date': None, 'enabled': True}  # sweep falls back to row expiry

        async def delete_panel_user(self, panel_user_id):
            deleted.append(panel_user_id)

        async def set_user_enabled(self, panel_user_id, enabled):
            enabled.append((panel_user_id, enabled))

        async def close(self):
            pass

    orig = tasks.AmneziaClient
    tasks.AmneziaClient = FakeClient
    sent = []
    tasks.notify_many_users = lambda ns: sent.extend(ns) or asyncio.sleep(0, result=None)
    try:
        sid = _seed_trial(777003, expired=True)
        tasks.check_amnezia_expiry.run()
        with SessionLocal() as db:
            from database import AmneziaService as S
            svc = db.query(S).filter(S.id == sid).first()
            assert svc.status == 'deleted', svc.status
        assert deleted == ['panel-777003']
        assert enabled == []          # trials are deleted, not disabled
        assert any('پایان رسید و حذف شد' in (n[1] or '') for n in sent), f'sent={sent!r}'
    finally:
        tasks.AmneziaClient = orig


def test_expiry_sweep_still_disables_paid_services():
    """Sanity: the trial branch must not change paid-service behaviour."""
    import asyncio
    from tests._bootstrap import bot as _b
    _clean_state()
    from database import SessionLocal, AmneziaService
    import tasks

    deleted, enabled_calls = [], []

    class FakeClient:
        async def get_user_stats(self, username):
            return None  # sweep falls back to row expiry_date

        async def set_user_enabled(self, panel_user_id, enabled):
            enabled_calls.append(panel_user_id)  # param shadows; renamed list

        async def delete_panel_user(self, panel_user_id):
            deleted.append(panel_user_id)

        async def close(self):
            pass

    orig = tasks.AmneziaClient
    tasks.AmneziaClient = FakeClient
    tasks.notify_many_users = lambda ns: asyncio.sleep(0, result=None)
    try:
        now = datetime.now(timezone.utc)
        with SessionLocal() as db:
            db.add(AmneziaService(
                telegram_user_id=777004, connection_id='c4', client_id='k4',
                server_id=0, protocol='awg2', name='paid_4',
                quota_bytes=1024 ** 3,
                expiry_date=now - timedelta(days=1),
                is_trial=False, panel_user_id='panel-4', panel_username='paid4'))
            db.commit()
        tasks.check_amnezia_expiry.run()
        with SessionLocal() as db:
            svc = db.query(AmneziaService).filter(
                AmneziaService.telegram_user_id == 777004).first()
            assert svc.status == 'expired'
        assert enabled_calls == ['panel-4'] and deleted == []
    finally:
        tasks.AmneziaClient = orig


def test_admin_submenu_has_reset_button():
    from src.utils.keyboard import get_admin_category_kb
    _, kb = get_admin_category_kb('admcat_amnezia')
    cbs = [b.callback_data for row in kb.inline_keyboard for b in row]
    assert 'admztrial_reset' in cbs and cbs[-1] == 'admin_panel'


def test_gift_screen_offers_amnezia_for_admin_only_when_disabled():
    import asyncio
    from tests._bootstrap import FakeState

    async def go(user_id):
        return await bot.free_trial_content(user_id=user_id)

    text, kb = asyncio.run(go(111))     # admin, AMNEZIA_ENABLED=false
    assert '🟣 تست رایگان Amnezia' in str([b.text for r in kb.inline_keyboard for b in r])
    texts = [b.callback_data for r in kb.inline_keyboard for b in r]
    assert 'free_trial_amz' in texts
    text2, kb2 = asyncio.run(go(999))   # regular user while disabled
    cbs2 = [b.callback_data for r in kb2.inline_keyboard for b in r]
    assert 'free_trial_amz' not in cbs2 and text2 == text2
