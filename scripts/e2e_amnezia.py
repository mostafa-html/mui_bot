"""End-to-end test: provision_amnezia_new → renew → topup → expiry against the live panel.
Uses a throwaway SQLite DB and disables all Telegram sending. Safe to delete."""
import os
import sys
import json
import asyncio
import logging
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Must be set BEFORE importing database/tasks (load_dotenv does not override).
os.environ['SKIP_DB_INIT'] = 'false'
os.environ['DATABASE_URL'] = 'sqlite:///storage/test_e2e.db'
os.environ['BOT_TOKEN'] = '123456:dummy-token-not-real'  # keeps src.config happy; sends fail harmlessly
os.environ['ADMIN_CHAT_IDS'] = ''
os.environ['AMNEZIA_API_URL'] = 'https://rahanet.aethera.ir:5000'
os.environ.setdefault('AMNEZIA_API_USERNAME', os.getenv('AMNEZIA_API_USERNAME', ''))
os.environ.setdefault('AMNEZIA_API_PASSWORD', os.getenv('AMNEZIA_API_PASSWORD', ''))

logging.basicConfig(level=logging.WARNING)

from database import SessionLocal, Plan, Invoice, AmneziaService, AmneziaUser, engine
from sqlalchemy import text

# Local-test workaround: SQLite cannot autoincrement BigInteger PKs
# (PostgreSQL generates BIGSERIAL and is unaffected). Swap just this one
# table to an INTEGER PK so the ORM inserts in tasks.py work locally.
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

# No Redis locally: stub the loading-message helpers (production always has Redis).
async def _no_op(*a, **k):
    return None
tasks.animate_loading_message = _no_op
tasks.update_loading_message = _no_op

TEST_TG = 999999999


def mk_invoice(action, plan_id=None, svc_id=None, added_gb=None):
    with SessionLocal() as db:
        inv = Invoice(
            id=next_id(),          # explicit id: BigInteger PK doesn't autoincrement on sqlite
            telegram_user_id=TEST_TG,
            plan_id=plan_id,
            added_gb=added_gb,
            total_price=1,
            original_price=1,
            client_name=None,
            action_type=action,
            status="PENDING",
            amnezia_service_id=svc_id,
            description=json.dumps({"server_id": 1}) if action == "AMNEZIA_NEW" else None,
        )
        db.add(inv)
        db.commit()
        db.refresh(inv)
        return inv.id


def svc_row():
    with SessionLocal() as db:
        s = db.query(AmneziaService).filter(AmneziaService.telegram_user_id == TEST_TG).first()
        return (s.id, s.connection_id, s.client_id, s.server_id, s.name,
                s.quota_bytes, s.expiry_date, s.status)


def inv_status(iid):
    with SessionLocal() as db:
        return db.query(Invoice).filter(Invoice.id == iid).first().status


_inv_counter = [991000]
def next_id():
    _inv_counter[0] += 1
    return _inv_counter[0]


with SessionLocal() as db:
    p = Plan(id=990001, name='E2E Amnezia 10GB', traffic_gb=10, duration_days=30, price=100000,
             is_active=True, service_type='amnezia')
    db.add(p)
    db.commit()
    db.refresh(p)
    plan_id = p.id
print('PLAN OK:', plan_id)

# ---------- 1) NEW ----------
iid1 = mk_invoice("AMNEZIA_NEW", plan_id=plan_id)
tasks.provision_amnezia_new.run(iid1)
assert inv_status(iid1) == "COMPLETE", f"NEW invoice status: {inv_status(iid1)}"
sid, conn_id, client_id, server_id, name, quota, expiry, status = svc_row()
print(f'NEW OK: service={sid} name={name} server={server_id} quota={quota} exp={expiry} status={status}')
assert quota == 10 * 1024 ** 3 and status == 'active'

# idempotency: re-running must not create a second connection
tasks.provision_amnezia_new.run(iid1)
assert svc_row()[0] == sid, "retry created a duplicate service row!"
print('IDEMPOTENCY OK')

# panel-side verification (fresh client per asyncio.run loop — one loop per call)
from src.services.amnezia import AmneziaClient, GB
with SessionLocal() as db:
    mapping = db.query(AmneziaService).filter(AmneziaService.telegram_user_id == TEST_TG).first()

async def _fresh_stats():
    c = AmneziaClient()
    try:
        return await c.get_user_stats(mapping.panel_username)
    finally:
        await c.close()

stats = asyncio.run(_fresh_stats())
print('PANEL after NEW: limit=%.2f GB exp=%s' % (stats['limit'] / GB, stats['expiration_date']))
assert abs(stats['limit'] - 10 * GB) < GB

# ---------- 2) RENEW (unused carries over; +10GB plan, +30 days) ----------
iid2 = mk_invoice("AMNEZIA_RENEW", plan_id=plan_id, svc_id=sid)
tasks.provision_amnezia_renew.run(iid2)
assert inv_status(iid2) == "COMPLETE", f"RENEW invoice status: {inv_status(iid2)}"
exp_after_renew = svc_row()[6]
print(f'RENEW OK: new expiry={exp_after_renew}')
assert (exp_after_renew - expiry).days >= 29, f"expiry not extended: {expiry} -> {exp_after_renew}"

# ---------- 3) TOPUP +5GB ----------
iid3 = mk_invoice("AMNEZIA_TOPUP", svc_id=sid, added_gb=5)
tasks.provision_amnezia_topup.run(iid3)
assert inv_status(iid3) == "COMPLETE", f"TOPUP invoice status: {inv_status(iid3)}"
topup_quota = svc_row()[5]
print(f'TOPUP OK: quota now={topup_quota / GB:.0f} GB')
# renew: 10GB unused carried + 10GB plan = 20; topup: +5 => 25
assert abs(topup_quota - 25 * GB) < GB

# ---------- 4) EXPIRY sweep (disable path) ----------
# The sweep trusts the PANEL's expiration_date, so expire it there.
async def _expire_on_panel():
    c = AmneziaClient()
    try:
        await c.update_user_limits(
            mapping.panel_user_id, TEST_TG,
            expiry=datetime.now(timezone.utc) - timedelta(days=1))
    finally:
        await c.close()
asyncio.run(_expire_on_panel())
tasks.check_amnezia_expiry.run()
with SessionLocal() as db:
    s = db.query(AmneziaService).filter(AmneziaService.id == sid).first()
    print(f'EXPIRY OK: status={s.status}')
    assert s.status == 'expired'
stats2 = asyncio.run(_fresh_stats())
print(f'PANEL disabled: enabled={stats2["enabled"]}')
assert stats2['enabled'] is False

# ---------- cleanup ----------
async def _cleanup():
    c = AmneziaClient()
    try:
        await c.delete_panel_user(mapping.panel_user_id)
    finally:
        await c.close()
asyncio.run(_cleanup())
print('PANEL CLEANUP OK')
print('ALL E2E ASSERTIONS PASSED')
