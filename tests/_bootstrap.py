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
