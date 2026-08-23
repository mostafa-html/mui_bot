"""Reorder-resilience test: pure reorder (id shift, same name) must re-anchor
SILENTLY; a physical move (name change) must re-anchor AND notify.
Throwaway SQLite DB, Telegram sends disabled. Safe to delete."""
import os
import sys
import json
import asyncio
import logging

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

os.environ['SKIP_DB_INIT'] = 'false'
os.environ['DATABASE_URL'] = 'sqlite:///storage/test_reorder.db'
os.environ['BOT_TOKEN'] = '123456:dummy-token-not-real'
os.environ['ADMIN_CHAT_IDS'] = ''
os.environ['REDIS_URL'] = 'redis://localhost:6390/0'   # unreachable → cache ops degrade gracefully
os.environ['AMNEZIA_API_URL'] = 'https://rahanet.aethera.ir:5000'
os.environ.setdefault('AMNEZIA_API_USERNAME', os.getenv('AMNEZIA_API_USERNAME', ''))
os.environ.setdefault('AMNEZIA_API_PASSWORD', os.getenv('AMNEZIA_API_PASSWORD', ''))

logging.basicConfig(level=logging.WARNING)

from database import SessionLocal, Plan, Invoice, AmneziaService, AmneziaUser, engine
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

TEST_TG = 999999666
NOTIFICATIONS = []
tasks.notify_many_users = lambda ns: NOTIFICATIONS.extend(ns) or _no_op()

with SessionLocal() as db:
    db.add(Plan(id=993001, name='Reorder 5GB', traffic_gb=5, duration_days=30, price=50000,
                is_active=True, service_type='amnezia'))
    db.commit()
    inv = Invoice(id=993100, telegram_user_id=TEST_TG, plan_id=993001, total_price=1,
                  original_price=1, action_type='AMNEZIA_NEW', status='PENDING',
                  description=json.dumps({"server_id": 1}))
    db.add(inv); db.commit()

tasks.provision_amnezia_new.run(993100)
with SessionLocal() as db:
    svc = db.query(AmneziaService).filter(AmneziaService.telegram_user_id == TEST_TG).first()
    mu = db.query(AmneziaService).filter(AmneziaService.telegram_user_id == TEST_TG).first()
    real_sid, real_name = svc.server_id, svc.server_name
    svc_row_id = svc.id
print(f'PROVISIONED on {real_name} (index {real_sid})')
NOTIFICATIONS.clear()   # drop the provisioning welcome message from the counter

# ---------- Scenario A: PURE REORDER (same name, index shifted) ----------
# After a reorder the panel gives our connection a NEW index while the NAME
# stays identical. Simulate by storing a stale index with the correct name.
with SessionLocal() as db:
    svc = db.query(AmneziaService).filter(AmneziaService.id == svc_row_id).first()
    stale_index = [i for i in (0, 1, 2) if i != real_sid][0]
    svc.server_id = stale_index
    db.commit()
print(f'SIMULATED REORDER: stored index={stale_index}, name kept "{real_name}"')

tasks.check_amnezia_servers.run()
with SessionLocal() as db:
    svc = db.query(AmneziaService).filter(AmneziaService.id == svc_row_id).first()
    print(f'AFTER SWEEP A: index={svc.server_id} name="{svc.server_name}" notifications={len(NOTIFICATIONS)}')
    assert svc.server_id == real_sid, f"reorder not re-anchored: {svc.server_id} != {real_sid}"
    assert svc.server_name == real_name
    assert len(NOTIFICATIONS) == 0, f"pure reorder should be SILENT, got: {NOTIFICATIONS}"
print('PURE-REORDER RE-ANCHOR OK (silent)')

# ---------- Scenario B: PHYSICAL MOVE (different name) ----------
with SessionLocal() as db:
    svc = db.query(AmneziaService).filter(AmneziaService.id == svc_row_id).first()
    svc.server_name = 'some-old-server'
    db.commit()
NOTIFICATIONS.clear()

tasks.check_amnezia_servers.run()
with SessionLocal() as db:
    svc = db.query(AmneziaService).filter(AmneziaService.id == svc_row_id).first()
    print(f'AFTER SWEEP B: index={svc.server_id} name="{svc.server_name}" notifications={len(NOTIFICATIONS)}')
    for n in NOTIFICATIONS:
        print('NOTIFICATION:', n[1][:90].replace('\n', ' | '))
    assert svc.server_name == real_name, f"move not re-anchored: {svc.server_name}"
    assert svc.server_id == real_sid
    assert len(NOTIFICATIONS) == 1, f"physical move should notify once: {NOTIFICATIONS}"
    assert 'تغییر کرد' in NOTIFICATIONS[0][1]
print('MOVE DETECTION + NOTIFICATION OK')

# ---------- cleanup ----------
async def cleanup():
    from src.services.amnezia import AmneziaClient, AmneziaError
    c = AmneziaClient()
    try:
        await c.delete_panel_user(mu.panel_user_id)
    except AmneziaError as e:
        if 'not found' in str(e).lower():
            print('(panel user already deleted — cleanup considered done)')
        else:
            raise
    finally:
        await c.close()
asyncio.run(cleanup())
print('PANEL CLEANUP OK')
print('ALL REORDER ASSERTIONS PASSED')
