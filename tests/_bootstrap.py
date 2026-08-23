"""Shared bootstrap for the unit-test suite.

Import this FIRST in every test module: it pins the environment before the
app modules load (they read env at import time) and provides tiny fakes for
aiogram's Message/FSMContext so flows can be exercised without Telegram.
Run everything via ``python -m tests.run_all`` (pytest also works).
"""
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

# Must be set BEFORE importing database/tasks/bot (load_dotenv won't override).
os.environ['SKIP_DB_INIT'] = 'false'
os.environ['DATABASE_URL'] = 'sqlite:///storage/test_unit.db'
os.environ['BOT_TOKEN'] = '123456:unit-test-dummy'
os.environ['ADMIN_CHAT_IDS'] = '111'
os.environ['REDIS_URL'] = 'redis://localhost:6390/0'   # unreachable → dedup falls back to memory
os.environ['AMNEZIA_API_URL'] = 'https://unit.test'
os.environ['AMNEZIA_API_USERNAME'] = 'unit'
os.environ['AMNEZIA_API_PASSWORD'] = 'unitpass'
os.environ['AMNEZIA_ENABLED'] = 'false'


class FakeState:
    """Minimal FSMContext stand-in: dict-backed update/get/set."""

    def __init__(self, initial=None):
        self.data = dict(initial or {})
        self.state = None

    async def update_data(self, **kw):
        self.data.update(kw)

    async def get_data(self):
        return self.data

    async def set_state(self, s):
        self.state = s

    async def clear(self):
        self.data.clear()
        self.state = None


class FakeSent:
    message_id = 42


class FakeMsg:
    """Message stand-in that records answers instead of sending anything."""

    def __init__(self):
        self.sent = []

    async def answer(self, text=None, *a, **k):
        self.sent.append(text)
        return FakeSent()

    async def edit_text(self, text=None, *a, **k):
        self.sent.append(text)
        return FakeSent()


def seed_plan(plan_id, name='Amz 10GB', gb=10, days=30, price=100000, service_type='amnezia'):
    from database import SessionLocal, Plan
    with SessionLocal() as db:
        if db.query(Plan).filter(Plan.id == plan_id).first() is None:
            db.add(Plan(id=plan_id, name=name, traffic_gb=gb, duration_days=days,
                        price=price, is_active=True, service_type=service_type))
            db.commit()


# Import the app modules LAST (after the env above is pinned) and re-export
# them so tests can simply do:  from tests._bootstrap import bot, tasks
import database  # noqa: E402,F401  (initialises schema on the sqlite file)
import tasks      # noqa: E402,F401
import bot        # noqa: E402,F401

# SQLite cannot autoincrement BigInteger PKs (PostgreSQL generates BIGSERIAL
# and is unaffected) — swap invoices to an INTEGER PK so handler tests can
# insert through the ORM exactly as production does.
from sqlalchemy import text  # noqa: E402
with database.engine.begin() as conn:
    conn.execute(text("DROP TABLE IF EXISTS invoices"))
    conn.execute(text("""
        CREATE TABLE invoices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            telegram_user_id BIGINT NOT NULL,
            plan_id BIGINT,
            added_gb BIGINT,
            total_price BIGINT,
            original_price BIGINT,
            discount_amount BIGINT,
            coupon_code VARCHAR,
            client_name VARCHAR,
            action_type VARCHAR NOT NULL,
            screenshot_local_path VARCHAR,
            status VARCHAR DEFAULT 'PENDING',
            created_at DATETIME,
            reseller_id BIGINT,
            reservation_data TEXT,
            refund_data TEXT,
            description VARCHAR,
            pack_id BIGINT,
            deletion_scheduled_at DATETIME,
            deletion_warning_sent_count INTEGER DEFAULT 0 NOT NULL,
            amnezia_service_id INTEGER
        )"""))
