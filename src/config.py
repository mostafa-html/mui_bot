import os
import logging
from typing import List

from dotenv import load_dotenv

load_dotenv()

# ========== Environment Variables ==========

# Validate required environment variables
REQUIRED_ENV_VARS = [
    'BOT_TOKEN',
    'REQUIRED_CHANNEL_ID',
    'REQUIRED_CHANNEL_LINK',
    'PANEL_URL',
    'PANEL_USERNAME',
    'PANEL_PASSWORD',
    'REDIS_URL',
]
missing = [var for var in REQUIRED_ENV_VARS if not os.getenv(var)]
if missing:
    raise EnvironmentError(f"Missing required environment variables: {', '.join(missing)}")

BOT_TOKEN = os.getenv('BOT_TOKEN')
REQUIRED_CHANNEL_ID = int(os.getenv('REQUIRED_CHANNEL_ID'))
REQUIRED_CHANNEL_LINK = os.getenv('REQUIRED_CHANNEL_LINK')
PANEL_URL = os.getenv('PANEL_URL').rstrip('/')
PANEL_USERNAME = os.getenv('PANEL_USERNAME')
PANEL_PASSWORD = os.getenv('PANEL_PASSWORD')
REDIS_URL = os.getenv('REDIS_URL')

# Optional variables with defaults
ADMIN_CHAT_IDS = os.getenv('ADMIN_CHAT_IDS', '')
DATABASE_URL = os.getenv('DATABASE_URL', 'sqlite:///storage/bot.db')
PANEL_SSL_VERIFY = os.getenv('PANEL_SSL_VERIFY', 'false').lower() == 'true'
SKIP_DB_INIT = os.getenv('SKIP_DB_INIT', 'false').lower() == 'true'

# ========== Amnezia Web Panel ==========
AMNEZIA_API_URL = os.getenv('AMNEZIA_API_URL', '').rstrip('/')
AMNEZIA_API_USERNAME = os.getenv('AMNEZIA_API_USERNAME', '')
AMNEZIA_API_PASSWORD = os.getenv('AMNEZIA_API_PASSWORD', '')
AMNEZIA_PROTOCOL = os.getenv('AMNEZIA_PROTOCOL', 'awg2')
AMNEZIA_ENABLED = os.getenv('AMNEZIA_ENABLED', 'false').lower() == 'true'
# When AMNEZIA_ENABLED is false only admins see/use Amnezia features.
AMNEZIA_AVAILABLE = bool(AMNEZIA_API_URL and AMNEZIA_API_USERNAME and AMNEZIA_API_PASSWORD)


def get_admin_ids() -> List[int]:
    """Return list of admin Telegram user IDs from environment."""
    return [int(x.strip()) for x in ADMIN_CHAT_IDS.split(',') if x.strip()]


def is_admin(user_id: int) -> bool:
    """Check if a user ID is in the admin list."""
    return user_id in get_admin_ids()


def amnezia_visible(user_id: int) -> bool:
    """True if Amnezia options should be shown to this user.

    Requires the integration to be configured; when AMNEZIA_ENABLED is off
    (experimental mode) only admins may see it.
    """
    return AMNEZIA_AVAILABLE and (AMNEZIA_ENABLED or is_admin(user_id))


# ========== Database Session ==========
# Import here to avoid circular imports, but we keep session creation in database.py
from database import SessionLocal  # noqa: E402

# Expose SessionLocal for convenience
__all__ = [
    'BOT_TOKEN',
    'REQUIRED_CHANNEL_ID',
    'REQUIRED_CHANNEL_LINK',
    'PANEL_URL',
    'PANEL_USERNAME',
    'PANEL_PASSWORD',
    'REDIS_URL',
    'DATABASE_URL',
    'PANEL_SSL_VERIFY',
    'SKIP_DB_INIT',
    'AMNEZIA_API_URL',
    'AMNEZIA_API_USERNAME',
    'AMNEZIA_API_PASSWORD',
    'AMNEZIA_PROTOCOL',
    'AMNEZIA_ENABLED',
    'get_admin_ids',
    'is_admin',
    'amnezia_visible',
    'SessionLocal',
]