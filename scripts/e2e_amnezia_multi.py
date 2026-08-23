"""Separate-accounts E2E: two purchases by ONE Telegram user must create two
independent panel accounts/quotas; renewing one must not touch the other.
Throwaway SQLite DB, Telegram sends disabled. Safe to delete."""
import os
import sys
import json
import asyncio
import logging

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

os.environ['SKIP_DB_INIT'] = 'false'
os.environ['DATABASE_URL'] = 'sqlite:///storage/test_multi.db'
os.environ['BOT_TOKEN'] = '123456:dummy-token-not-real'
os.environ['ADMIN_CHAT_IDS'] = ''
os.environ['REDIS_URL'] = 'redis://localhost:6390/0'
os.environ['AMNEZIA_API_URL'] = 'https://rahanet.aethera.ir:5000'
# credentials come from .env via load_dotenv
# credentials come from .env via load_dotenv

logging.basicConfig(level=logging.WARNING)

from database import SessionLocal, Plan, Invoice, AmneziaService, engine
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

TEST_TG = 999999555
from src.services.amnezia import AmneziaClient, GB

with SessionLocal() as db:
    db.add(Plan(id=994001, name='Multi A 10GB', traffic_gb=10, duration_days=30,
                price=100000, is_active=True, service_type='amnezia'))
    db.add(Plan(id=994002, name='Multi B 25GB', traffic_gb=25, duration_days=60,
                price=250000, is_active=True, service_type='amnezia'))
    db.commit()

def mk_inv(iid, plan_id, action, creds=None, svc_id=None):
    desc = {"server_id": 0}
    if creds:
        desc.update({"amz_username": creds[0], "amz_password": creds[1]})
    with SessionLocal() as db:
        db.add(Invoice(id=iid, telegram_user_id=TEST_TG, plan_id=plan_id, total_price=1,
                       original_price=1, action_type=action, status='PENDING',
                       amnezia_service_id=svc_id,
                       description=json.dumps(desc)))
        db.commit()

def svcs():
    with SessionLocal() as db:
        rows = db.query(AmneziaService).filter(
            AmneziaService.telegram_user_id == TEST_TG).order_by(AmneziaService.id).all()
        return [(r.id, r.panel_user_id, r.panel_username, r.panel_password,
                 r.quota_bytes, r.expiry_date) for r in rows]

# ---- purchase 1: custom creds ----
mk_inv(994100, 994001, 'AMNEZIA_NEW', creds=('ali_multi', 'PassOne77'))
tasks.provision_amnezia_new.run(994100)
# ---- purchase 2: same user, auto creds, different server ----
mk_inv(994101, 994002, 'AMNEZIA_NEW')
os.environ['AMNEZIA_FORCE_SERVER'] = ''  # (description server_id honored below)
with SessionLocal() as db:  # point second purchase at server 1
    inv = db.query(Invoice).filter(Invoice.id == 994101).first()
    inv.description = json.dumps({"server_id": 1})
    db.commit()
tasks.provision_amnezia_new.run(994101)

rows = svcs()
print('SUBS:', [(r[2], f'{r[4]//GB}GB') for r in rows])
assert len(rows) == 2, 'expected two services'
s1, s2 = rows
assert s1[1] != s2[1], 'panel accounts must differ!'
assert s1[2] == 'ali_multi_994100' and s1[3] == 'PassOne77'
assert s2[2].startswith('tg_999999555_994101')
assert s1[4] == 10 * GB and s2[4] == 25 * GB
print('TWO INDEPENDENT ACCOUNTS OK')

# ---- panel-side: each account isolated ----
async def stats_for(username):
    c = AmneziaClient()
    try:
        return await c.get_user_stats(username)
    finally:
        await c.close()
st1 = asyncio.run(stats_for(s1[2]))
st2 = asyncio.run(stats_for(s2[2]))
print(f'PANEL: {s1[2]}={st1["limit"]//GB}GB/{st1["expiration_date"].date()}  '
      f'{s2[2]}={st2["limit"]//GB}GB/{st2["expiration_date"].date()}')
assert st1['limit'] == 10 * GB and st2['limit'] == 25 * GB
assert st1['connections_count'] == 1 and st2['connections_count'] == 1
print('PANEL ISOLATION OK')

# ---- renew #1: #2 must stay untouched ----
mk_inv(994102, 994001, 'AMNEZIA_RENEW', svc_id=s1[0])
tasks.provision_amnezia_renew.run(994102)
rows = svcs()
st2_after = asyncio.run(stats_for(s2[2]))
assert rows[1][4] == 25 * GB, 'service #2 quota changed!'
assert st2_after['limit'] == 25 * GB, 'service #2 panel quota changed!'
assert (rows[0][5] - s1[5]).days >= 29
print('RENEW ISOLATION OK')

# ---- topup #1: #2 untouched ----
with SessionLocal() as db:
    db.add(Invoice(id=994103, telegram_user_id=TEST_TG, plan_id=994001, added_gb=5,
                   total_price=1, original_price=1, action_type='AMNEZIA_TOPUP',
                   status='PENDING', amnezia_service_id=s1[0]))
    db.commit()
tasks.provision_amnezia_topup.run(994103)
rows = svcs()
# renew made #1 = 10 (unused) + 10 (plan) = 20GB; topup +5 => 25GB. #2 untouched.
assert rows[0][4] == 25 * GB and rows[1][4] == 25 * GB
print('TOPUP ISOLATION OK')

# ---- sweeps run clean on multi-service state ----
tasks.check_amnezia_servers.run()
tasks.check_amnezia_expiry.run()
rows = svcs()
assert all(r for r in rows)
print('SWEEPS OK')

# ---- delete service #1: account cascades, #2 intact ----
tasks.delete_amnezia_service.run(s1[0])
rows = svcs()
with SessionLocal() as db:
    st1row = db.query(AmneziaService).filter(AmneziaService.id == s1[0]).first()
    assert st1row.status == 'deleted'
st2_final = asyncio.run(stats_for(s2[2]))
assert st2_final['limit'] == 25 * GB and st2_final['enabled']
print('DELETE ISOLATION OK')

# ---- cleanup ----
async def cleanup():
    from src.services.amnezia import AmneziaError
    c = AmneziaClient()
    try:
        for uid in (s1[1], s2[1]):
            try:
                await c.delete_panel_user(uid)
            except AmneziaError as e:
                if 'not found' not in str(e).lower():
                    raise
    finally:
        await c.close()
asyncio.run(cleanup())
print('PANEL CLEANUP OK')
print('ALL MULTI-ACCOUNT ASSERTIONS PASSED')
