"""Live E2E: Amnezia trial lifecycle against the real panel.
provision → verify account/connection/claim → force-expire on panel →
sweep → assert panel account AND connection deleted, service row 'deleted'.
Throwaway sqlite; Telegram sends disabled. Safe to delete."""
import os
import sys
import asyncio
import logging
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

os.environ['SKIP_DB_INIT'] = 'false'
os.environ['DATABASE_URL'] = 'sqlite:///storage/test_trial.db'
os.environ['BOT_TOKEN'] = '123456:dummy-token-not-real'
os.environ['ADMIN_CHAT_IDS'] = ''
os.environ['REDIS_URL'] = 'redis://localhost:6390/0'
os.environ['AMNEZIA_ENABLED'] = 'false'

logging.basicConfig(level=logging.WARNING)

from database import SessionLocal, AmneziaService, AmneziaTrial, Invoice, engine
from sqlalchemy import text

with engine.begin() as conn:
    for tbl in ('amnezia_services', 'invoices'):
        conn.execute(text(f"DROP TABLE IF EXISTS {tbl}"))
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
    conn.execute(text("""
        CREATE TABLE invoices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            telegram_user_id BIGINT NOT NULL,
            plan_id BIGINT, added_gb BIGINT, total_price BIGINT, original_price BIGINT,
            discount_amount BIGINT, coupon_code VARCHAR, client_name VARCHAR,
            action_type VARCHAR NOT NULL, screenshot_local_path VARCHAR,
            status VARCHAR DEFAULT 'PENDING', created_at DATETIME, reseller_id BIGINT,
            reservation_data TEXT, refund_data TEXT, description VARCHAR, pack_id BIGINT,
            deletion_scheduled_at DATETIME, deletion_warning_sent_count INTEGER DEFAULT 0 NOT NULL,
            amnezia_service_id INTEGER
        )"""))

import tasks

async def _no_op(*a, **k):
    return None
tasks.animate_loading_message = _no_op
tasks.update_loading_message = _no_op

TEST_TG = 999444333
from src.services.amnezia import AmneziaClient, AmneziaError, GB

# ---------- 1) provision ----------
tasks.provision_amnezia_trial.run(TEST_TG)
with SessionLocal() as db:
    svc = db.query(AmneziaService).filter(AmneziaService.telegram_user_id == TEST_TG).first()
    claim = db.query(AmneziaTrial).filter(AmneziaTrial.telegram_user_id == TEST_TG).first()
    inv = db.query(Invoice).filter(Invoice.telegram_user_id == TEST_TG).first()
    assert svc and claim and inv, 'trial rows missing'
    print(f'PROVISION OK: name={svc.name} is_trial={svc.is_trial} '
          f'server={svc.server_name} account={svc.panel_username} '
          f'claim.service={claim.service_id == svc.id} invoice={inv.action_type}')
    assert svc.is_trial and inv.action_type == 'AMNEZIA_TRIAL' and inv.status == 'COMPLETE'
    panel_uid, panel_user = svc.panel_user_id, svc.panel_username
    svc_id, conn_id = svc.id, svc.connection_id

# ---------- 2) panel-side truth ----------
async def stats_of(username):
    c = AmneziaClient()
    try:
        return await c.get_user_stats(username)
    finally:
        await c.close()
st = asyncio.run(stats_of(panel_user))
print(f'PANEL: enabled={st["enabled"]} conns={st["connections_count"]} limit>={1 * GB}')
assert st['enabled'] and st['connections_count'] == 1 and st['limit'] >= GB

# ---------- 3) claim twice → guard blocks (task-level re-check) ----------
tasks.provision_amnezia_trial.run(TEST_TG)
with SessionLocal() as db:
    n = db.query(AmneziaService).filter(AmneziaService.telegram_user_id == TEST_TG).count()
assert n == 1, f'duplicate trial service created: {n}'
print('ONE-TRIAL GUARD OK')

# ---------- 4) expire on panel, sweep deletes everything ----------
async def expire():
    c = AmneziaClient()
    try:
        await c.update_user_limits(panel_uid, TEST_TG,
                                   expiry=datetime.now(timezone.utc) - timedelta(days=1))
    finally:
        await c.close()
asyncio.run(expire())
tasks.check_amnezia_expiry.run()
with SessionLocal() as db:
    svc = db.query(AmneziaService).filter(AmneziaService.id == svc_id).first()
    print(f'SWEEP: status={svc.status}')
    assert svc.status == 'deleted'

async def gone():
    c = AmneziaClient()
    try:
        return await c.get_user_stats(panel_user)
    finally:
        await c.close()
assert asyncio.run(gone()) is None, 'panel account still alive!'
print('PANEL ACCOUNT + CONNECTION DELETED OK (anti-population verified)')

# ---------- 5) reset-all clears entitlements ----------
import bot  # after env pinned
with SessionLocal() as db:
    db.add(AmneziaTrial(telegram_user_id=1))
    db.add(AmneziaTrial(telegram_user_id=2))
    db.commit()
    n = bot._reset_amnezia_trials(db)
    db.commit()
assert n >= 2
with SessionLocal() as db:
    assert db.query(AmneziaTrial).count() == 0
print('RESET-ALL OK')
print('ALL TRIAL E2E ASSERTIONS PASSED')
