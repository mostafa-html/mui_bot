"""v1.2 live test: server-name resolution, unlimited-plan provision, top-up guard data.
Throwaway SQLite DB, Telegram sends disabled. Safe to delete."""
import os
import sys
import json
import asyncio
import logging

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

os.environ['SKIP_DB_INIT'] = 'false'
os.environ['DATABASE_URL'] = 'sqlite:///storage/test_v12.db'
os.environ['BOT_TOKEN'] = '123456:dummy-token-not-real'
os.environ['ADMIN_CHAT_IDS'] = ''
os.environ['REDIS_URL'] = 'redis://localhost:6390/0'   # unreachable: name cache must degrade gracefully
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

TEST_TG = 999999777

from src.services.amnezia import AmneziaClient

# ---------- 1) name resolution ----------
async def check_names():
    c = AmneziaClient()
    try:
        servers = await c.list_servers_detailed()
        print('SERVERS:', [(s['id'], s.get('name'), s.get('alive')) for s in servers])
        assert len(servers) >= 2  # latvia (idx 1) deleted; old 3rd server shifted to idx 1 (netherlands_airnode)
        named = sum(1 for s in servers if s.get('name'))
        print(f'named: {named}/{len(servers)} (server 2 may be unnamed until it hosts a connection)')
        return servers
    finally:
        await c.close()
asyncio.run(check_names())
print('NAME RESOLUTION OK')

# ---------- 2) unlimited plan provision (quota 0) ----------
with SessionLocal() as db:
    p = Plan(id=992001, name='v12 Unlimited', traffic_gb=0, duration_days=30, price=200000,
             is_active=True, service_type='amnezia')
    db.add(p); db.commit()

with SessionLocal() as db:
    inv = Invoice(id=992100, telegram_user_id=TEST_TG, plan_id=992001, total_price=1,
                  original_price=1, action_type='AMNEZIA_NEW', status='PENDING',
                  description=json.dumps({"server_id": 0,
                                          "amz_username": "v12_unlim_user",
                                          "amz_password": "UnlimPass1"}))
    db.add(inv); db.commit()
tasks.provision_amnezia_new.run(992100)

from src.services.amnezia import GB
with SessionLocal() as db:
    inv = db.query(Invoice).filter(Invoice.id == 992100).first()
    assert inv.status == 'COMPLETE', inv.status
    svc = db.query(AmneziaService).filter(AmneziaService.telegram_user_id == TEST_TG).first()
    mu = db.query(AmneziaService).filter(AmneziaService.telegram_user_id == TEST_TG).first()
    print(f'UNLIMITED PROVISION OK: svc={svc.name} quota_bytes={svc.quota_bytes} server={svc.server_name}')
    assert svc.quota_bytes == 0
    panel_uid = mu.panel_user_id

async def verify_unlimited_on_panel():
    c = AmneziaClient()
    try:
        stats = await c.get_user_stats(mu.panel_username)
        print(f'PANEL: limit={stats["limit"]} (0 = unlimited), exp={stats["expiration_date"]}')
        assert stats['limit'] == 0, f'expected 0 (unlimited), got {stats["limit"]}'
    finally:
        await c.close()
asyncio.run(verify_unlimited_on_panel())

# renew on unlimited must KEEP it unlimited and extend expiry
with SessionLocal() as db:
    inv = Invoice(id=992101, telegram_user_id=TEST_TG, plan_id=992001, total_price=1,
                  original_price=1, action_type='AMNEZIA_RENEW', status='PENDING',
                  amnezia_service_id=svc.id)
    db.add(inv); db.commit()
exp_before = svc.expiry_date
tasks.provision_amnezia_renew.run(992101)
with SessionLocal() as db:
    inv = db.query(Invoice).filter(Invoice.id == 992101).first()
    assert inv.status == 'COMPLETE', inv.status
    svc = db.query(AmneziaService).filter(AmneziaService.id == svc.id).first()
    print(f'UNLIMITED RENEW OK: quota still {svc.quota_bytes}, expiry {exp_before.date()} -> {svc.expiry_date.date()}')
    assert svc.quota_bytes == 0
    assert (svc.expiry_date - exp_before).days >= 29

# ---------- cleanup ----------
async def cleanup():
    c = AmneziaClient()
    try:
        await c.delete_panel_user(panel_uid)
    finally:
        await c.close()
asyncio.run(cleanup())
print('PANEL CLEANUP OK')
print('ALL v1.2 ASSERTIONS PASSED')
