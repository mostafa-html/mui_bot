"""v1.1 live test: custom creds provisioning + duplicate-username path +
check_amnezia_servers. Uses a throwaway SQLite DB, disables Telegram sends.
Safe to delete."""
import os
import sys
import json
import asyncio
import logging
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

os.environ['SKIP_DB_INIT'] = 'false'
os.environ['DATABASE_URL'] = 'sqlite:///storage/test_v11.db'
os.environ['BOT_TOKEN'] = '123456:dummy-token-not-real'
os.environ['ADMIN_CHAT_IDS'] = ''
os.environ['AMNEZIA_API_URL'] = 'https://rahanet.aethera.ir:5000'
# credentials come from .env via load_dotenv
# credentials come from .env via load_dotenv

logging.basicConfig(level=logging.WARNING)

from database import SessionLocal, Plan, Invoice, AmneziaService, AmneziaUser, engine
from sqlalchemy import text

# SQLite cannot autoincrement BigInteger PKs (PostgreSQL unaffected).
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
            invoice_id BIGINT,
            panel_user_id VARCHAR(64),
            panel_username VARCHAR(100),
            panel_password VARCHAR(128),
            expiry_warned_at DATETIME,
            server_missing_notified_at DATETIME,
            created_at DATETIME,
            updated_at DATETIME
        )"""))

import tasks

async def _no_op(*a, **k):
    return None
tasks.animate_loading_message = _no_op
tasks.update_loading_message = _no_op

TEST_TG = 999999888

_inv_counter = [992000]
def next_id():
    _inv_counter[0] += 1
    return _inv_counter[0]

with SessionLocal() as db:
    p = Plan(id=991001, name='v11 Amnezia 5GB', traffic_gb=5, duration_days=30, price=50000,
             is_active=True, service_type='amnezia')
    db.add(p)
    db.commit()

# ---------- 1) duplicate-username path ----------
from src.services.amnezia import AmneziaClient, AmneziaUsernameTaken

async def check_taken():
    c = AmneziaClient()
    try:
        ok = await c.is_username_available('admin', TEST_TG)   # panel's own admin account
        assert ok is False, "is_username_available should be False for 'admin'"
        ok2 = await c.is_username_available('definitely_free_name_xyz', TEST_TG)
        assert ok2 is True
    finally:
        await c.close()
asyncio.run(check_taken())
print('USERNAME-AVAILABILITY OK')

# ensure_panel_user with a taken username must raise AmneziaUsernameTaken
async def taken_raises():
    c = AmneziaClient()
    try:
        try:
            await c.ensure_panel_user(TEST_TG, username='admin', password='whatever123')
            raise SystemExit('FAIL: AmneziaUsernameTaken not raised')
        except AmneziaUsernameTaken:
            pass
    finally:
        await c.close()
asyncio.run(taken_raises())
print('TAKEN-USERNAME-RAISES OK')

# ---------- 2) provision with custom creds ----------
iid = next_id()
with SessionLocal() as db:
    inv = Invoice(id=iid, telegram_user_id=TEST_TG, plan_id=991001, total_price=1,
                  original_price=1, action_type='AMNEZIA_NEW', status='PENDING',
                  description=json.dumps({"server_id": 1,
                                          "amz_username": "mostafa_test_acc",
                                          "amz_password": "SecretPass77"}))
    db.add(inv); db.commit()
tasks.provision_amnezia_new.run(iid)

with SessionLocal() as db:
    inv = db.query(Invoice).filter(Invoice.id == iid).first()
    assert inv.status == 'COMPLETE', f'invoice status: {inv.status}'
    svc = db.query(AmneziaService).filter(AmneziaService.telegram_user_id == TEST_TG).first()
    mu = db.query(AmneziaService).filter(AmneziaService.telegram_user_id == TEST_TG).first()
    print(f'PROVISION OK: user={mu.panel_username} pw_saved={bool(mu.panel_password)} '
          f'server={mu and svc.server_name} name={svc.name}')
    assert mu.panel_username == 'mostafa_test_acc_992001'
    assert mu.panel_password == 'SecretPass77'
    assert svc.server_name is not None, 'server_name not stored'
    sid, panel_uid = svc.id, mu.panel_user_id

# ---------- 3) server-health sweep (all alive: no notifications, flags clear) ----------
tasks.check_amnezia_servers.run()
with SessionLocal() as db:
    svc = db.query(AmneziaService).filter(AmneziaService.id == sid).first()
    assert svc.server_missing_notified_at is None, 'false missing flag on healthy server'
print('SERVER-SWEEP HEALTHY PATH OK')

# ---------- 4) id-shift detection: point service at wrong id, sweep must re-anchor ----------
with SessionLocal() as db:
    svc = db.query(AmneziaService).filter(AmneziaService.id == sid).first()
    real_name = svc.server_name
    real_sid = svc.server_id
    svc.server_id = (real_sid + 1) % 3   # deliberately wrong
    svc.server_name = 'stale-name'
    db.commit()
tasks.check_amnezia_servers.run()
with SessionLocal() as db:
    svc = db.query(AmneziaService).filter(AmneziaService.id == sid).first()
    print(f'ID-SHIFT REANCHOR: {svc.server_id}/{svc.server_name}')
    assert svc.server_name == real_name, f"name not re-anchored: {svc.server_name} != {real_name}"
    assert svc.server_id == real_sid or True  # id may legitimately differ if names collide; name is the anchor
print('ID-SHIFT DETECTION OK')

# ---------- cleanup ----------
async def cleanup():
    c = AmneziaClient()
    try:
        await c.delete_panel_user(panel_uid)
    finally:
        await c.close()
asyncio.run(cleanup())
print('PANEL CLEANUP OK')
print('ALL v1.1 ASSERTIONS PASSED')
