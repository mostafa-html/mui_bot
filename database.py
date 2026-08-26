import os
import time
import logging
from datetime import datetime, timezone
from sqlalchemy import create_engine, Column, Integer, String, Boolean, DateTime, event, ForeignKey, text, inspect, BigInteger, JSON
from sqlalchemy.orm import declarative_base, sessionmaker

logger = logging.getLogger(__name__)

os.makedirs('./storage/receipts', exist_ok=True)

DATABASE_URL = os.getenv('DATABASE_URL', 'sqlite:///storage/bot.db')
IS_SQLITE = DATABASE_URL.startswith('sqlite:')

engine_kwargs = {}
if IS_SQLITE:
    engine_kwargs['connect_args'] = {'check_same_thread': False}
else:
    engine_kwargs['pool_pre_ping'] = True

engine = create_engine(DATABASE_URL, **engine_kwargs)

if IS_SQLITE:
    @event.listens_for(engine, 'connect')
    def set_sqlite_pragma(dbapi_connection, connection_record):
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.execute("PRAGMA synchronous=NORMAL")
        cursor.close()

Base = declarative_base()
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

class AppSetting(Base):
    __tablename__ = "settings"
    key = Column(String, primary_key=True, index=True)
    value = Column(String, nullable=False)

class Plan(Base):
    __tablename__ = "plans"
    id = Column(BigInteger, primary_key=True, index=True, autoincrement=True)
    name = Column(String, nullable=False)
    traffic_gb = Column(BigInteger, nullable=False)
    duration_days = Column(BigInteger, nullable=False)
    price = Column(BigInteger, nullable=False)
    is_active = Column(Boolean, default=True)
    service_type = Column(String, default='xui', nullable=False)  # 'xui' or 'amnezia'

class Invoice(Base):
    __tablename__ = "invoices"
    id = Column(BigInteger, primary_key=True, index=True, autoincrement=True)
    telegram_user_id = Column(BigInteger, index=True, nullable=False)
    plan_id = Column(BigInteger, nullable=True)
    added_gb = Column(BigInteger, nullable=True)
    total_price = Column(BigInteger, nullable=True)
    original_price = Column(BigInteger, nullable=True)
    discount_amount = Column(BigInteger, nullable=True)
    coupon_code = Column(String, nullable=True)
    client_name = Column(String, nullable=True)
    action_type = Column(String, nullable=False)  # NEW, RENEW, TOPUP, TRIAL, REFERRAL_REWARD, RESELLER_NEW/RENEW/TOPUP/EXTEND, MANUAL_RECEIPT, RESELLER_PACK_BUY, PANEL_SYNC, AMNEZIA_NEW/RENEW/TOPUP
    screenshot_local_path = Column(String, nullable=True)
    status = Column(String, default="PENDING")
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    reseller_id = Column(BigInteger, index=True, nullable=True)  # owning reseller for RESELLER_* invoices
    reservation_data = Column(String, nullable=True)  # JSON: {'packs': [{id, deducted}], 'legacy': deducted}
    refund_data = Column(JSON, nullable=True)
    description = Column(String, nullable=True)  # optional custom receipt description
    pack_id = Column(BigInteger, nullable=True, index=True)  # for RESELLER_PACK_BUY invoices
    amnezia_service_id = Column(BigInteger, nullable=True, index=True)  # for AMNEZIA_RENEW/TOPUP invoices
    # Deletion tracking fields
    deletion_scheduled_at = Column(DateTime(timezone=True), nullable=True)  # When service scheduled for deletion (7 days after expiry)
    deletion_warning_sent_count = Column(Integer, default=0, nullable=False, server_default='0')  # Number of warning messages sent (max 3)

class TrialUsage(Base):
    __tablename__ = "trial_usage"
    id = Column(BigInteger, primary_key=True, index=True, autoincrement=True)
    telegram_user_id = Column(BigInteger, unique=True, nullable=False)
    last_trial_date = Column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))
    service_email = Column(String, nullable=False)

class Coupon(Base):
    __tablename__ = "coupons"
    id = Column(BigInteger, primary_key=True, index=True, autoincrement=True)
    code = Column(String, unique=True, nullable=False, index=True)
    discount_type = Column(String, nullable=False)  # 'percent' or 'fixed'
    discount_value = Column(BigInteger, nullable=False)
    max_uses_total = Column(BigInteger, nullable=False, default=0)
    max_uses_per_user = Column(BigInteger, nullable=False, default=0)
    expiry_date = Column(DateTime(timezone=True), nullable=True)
    active = Column(Boolean, default=True)
    applicable_to = Column(String, nullable=False, default='all')  # 'all' or comma-separated: 'new,renewal,topup'
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))

class CouponUsage(Base):
    __tablename__ = "coupon_usage"
    id = Column(BigInteger, primary_key=True, index=True, autoincrement=True)
    coupon_id = Column(BigInteger, ForeignKey('coupons.id'), nullable=False)
    user_id = Column(BigInteger, nullable=False)
    used_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    invoice_id = Column(BigInteger, ForeignKey('invoices.id'), nullable=False)

class ReferralCode(Base):
    __tablename__ = "referral_codes"
    id = Column(BigInteger, primary_key=True, index=True, autoincrement=True)
    telegram_user_id = Column(BigInteger, unique=True, nullable=False)
    code = Column(String, unique=True, nullable=False, index=True)
    reward_claimed = Column(Boolean, default=False)

class Referral(Base):
    __tablename__ = "referrals"
    id = Column(BigInteger, primary_key=True, index=True, autoincrement=True)
    referrer_id = Column(BigInteger, ForeignKey('referral_codes.telegram_user_id'), nullable=False)
    referred_user_id = Column(BigInteger, unique=True, nullable=False)
    referred_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    became_paid = Column(Boolean, default=False)
    paid_at = Column(DateTime(timezone=True), nullable=True)  # set once in mark_referral_paid

class ReferralEvent(Base):
    __tablename__ = "referral_events"
    id = Column(BigInteger, primary_key=True, index=True, autoincrement=True)
    title = Column(String, nullable=False, default="رویداد معرفی")
    required_invites = Column(Integer, nullable=False)
    service_type = Column(String, nullable=False)  # 'vless' | 'amnezia'
    vless_plan_id = Column(BigInteger, ForeignKey('plans.id'), nullable=True)
    amnezia_gb = Column(Integer, nullable=True)     # whole GB; sub-GB rounded up at provisioning
    amnezia_days = Column(Integer, nullable=True)
    starts_at = Column(DateTime(timezone=True), nullable=False)
    ends_at = Column(DateTime(timezone=True), nullable=False)
    is_active = Column(Boolean, default=True)


class ReferralEventReward(Base):
    __tablename__ = "referral_event_rewards"
    id = Column(BigInteger, primary_key=True, index=True, autoincrement=True)
    event_id = Column(BigInteger, ForeignKey('referral_events.id'), nullable=False, index=True)
    referrer_id = Column(BigInteger, nullable=False, index=True)
    granted_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    service_type = Column(String, nullable=False)

class Reseller(Base):
    __tablename__ = "resellers"
    telegram_user_id = Column(BigInteger, primary_key=True, index=True)
    allowance_bytes = Column(BigInteger, nullable=False, default=0)  # total granted
    used_bytes = Column(BigInteger, nullable=False, default=0)       # consumed (reserved at create)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    # Retained for schema compatibility. Runtime provisioning uses only the
    # global reseller_inbound_ids AppSetting.
    inbound_ids = Column(String, nullable=True)

class ResellerPack(Base):
    __tablename__ = "reseller_packs"
    id = Column(BigInteger, primary_key=True, index=True, autoincrement=True)
    reseller_id = Column(BigInteger, ForeignKey('resellers.telegram_user_id', ondelete='CASCADE'), nullable=False, index=True)
    granted_bytes = Column(BigInteger, nullable=False)
    used_bytes = Column(BigInteger, nullable=False, default=0)
    expiry_date = Column(DateTime(timezone=True), nullable=False)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    is_active = Column(Boolean, default=True)   # becomes False when expired or fully used

class AmneziaTrial(Base):
    """One Amnezia free-trial entitlement per Telegram user (independent of
    the XUI TrialUsage records). Admin reset-all empties this table."""
    __tablename__ = "amnezia_trials"
    telegram_user_id = Column(BigInteger, primary_key=True, index=True)
    service_id = Column(BigInteger, nullable=True, index=True)  # the trial AmneziaService
    claimed_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))


class PanelTraffic(Base):
    __tablename__ = "panel_traffic"
    id = Column(BigInteger, primary_key=True, index=True, autoincrement=True)
    date = Column(DateTime(timezone=True), nullable=False, unique=True, index=True)
    cumulative_up = Column(BigInteger, nullable=False)
    cumulative_down = Column(BigInteger, nullable=False)
    daily_up = Column(BigInteger, nullable=False)
    daily_down = Column(BigInteger, nullable=False)

class TrafficPack(Base):
    __tablename__ = "traffic_packs"
    id = Column(BigInteger, primary_key=True, index=True, autoincrement=True)
    name = Column(String, nullable=False)
    traffic_gb = Column(BigInteger, nullable=False)
    price = Column(BigInteger, nullable=False)
    duration_days = Column(BigInteger, nullable=False)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))

class AmneziaUser(Base):
    """Maps a Telegram user to their Amnezia panel account (one per Telegram user)."""
    __tablename__ = "amnezia_users"
    telegram_user_id = Column(BigInteger, primary_key=True, index=True)
    panel_user_id = Column(String(64), unique=True, nullable=False, index=True)  # UUID from panel
    username = Column(String(64), unique=True, nullable=False, index=True)       # panel username
    panel_password = Column(String(128), nullable=True)  # kept so the bot can show it back (panel web-UI login)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))

class AmneziaService(Base):
    """One Amnezia connection (subscription) owned by a Telegram user."""
    __tablename__ = "amnezia_services"
    id = Column(BigInteger, primary_key=True, index=True, autoincrement=True)
    telegram_user_id = Column(BigInteger, index=True, nullable=False)
    connection_id = Column(String(64), unique=True, nullable=False, index=True)  # UUID from panel
    client_id = Column(String(255), nullable=False)  # base64 key used for config fetch
    server_id = Column(BigInteger, nullable=False)
    server_name = Column(String(100), nullable=True)  # identity anchor: panel server ids are list indexes and shift on delete/reorder
    protocol = Column(String(20), nullable=False, default='awg2')
    name = Column(String(100), nullable=False)
    quota_bytes = Column(BigInteger, nullable=False)          # total traffic limit (bytes)
    expiry_date = Column(DateTime(timezone=True), nullable=False)
    status = Column(String(20), default='active', nullable=False)  # active/expired/deleted
    is_trial = Column(Boolean, default=False, nullable=False)  # trials are DELETED (account+connection) at expiry to keep the panel lean
    invoice_id = Column(BigInteger, nullable=True, index=True)     # creating invoice
    # Each subscription owns a DEDICATED panel account: quota/expiry and
    # enable/disable are panel-account-level, so isolation requires 1:1
    # service <-> panel user (multiple purchases = multiple accounts).
    panel_user_id = Column(String(64), nullable=True, index=True)
    panel_username = Column(String(100), nullable=True)
    panel_password = Column(String(128), nullable=True)  # shown back to the owner
    expiry_warned_at = Column(DateTime(timezone=True), nullable=True)  # last warning sent (once per service)
    server_missing_notified_at = Column(DateTime(timezone=True), nullable=True)  # set when "server down/deleted" was sent; cleared when back
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc),
                        onupdate=lambda: datetime.now(timezone.utc))

def run_migrations():
    """Add missing columns to existing tables (schema migrations)."""
    inspector = inspect(engine)
    existing_tables = set(inspector.get_table_names())
    if 'invoices' not in existing_tables or 'coupons' not in existing_tables:
        return
    # For PostgreSQL: update the reseller_packs foreign key to ON DELETE CASCADE
    if 'reseller_packs' in existing_tables and not IS_SQLITE:
        with engine.connect() as conn:
            # Find the foreign key constraint name
            result = conn.execute(text("""
                SELECT conname
                FROM pg_constraint
                WHERE conrelid = 'reseller_packs'::regclass
                AND contype = 'f'
                AND confrelid = 'resellers'::regclass
            """))
            row = result.fetchone()
            if row:
                fk_name = row[0]
                # Check if it already has ON DELETE CASCADE by inspecting the definition
                # If not, drop and recreate
                # We'll just recreate to be safe
                conn.execute(text(f"ALTER TABLE reseller_packs DROP CONSTRAINT {fk_name}"))
                conn.execute(text(f"ALTER TABLE reseller_packs ADD CONSTRAINT {fk_name} FOREIGN KEY (reseller_id) REFERENCES resellers(telegram_user_id) ON DELETE CASCADE"))
                conn.commit()

    # Invoice table migrations
    invoice_columns = [col['name'] for col in inspector.get_columns('invoices')]
    invoice_additions = {
        'original_price': 'INTEGER',
        'discount_amount': 'INTEGER',
        'coupon_code': 'VARCHAR',
        'added_gb': 'INTEGER',
        'reseller_id': 'INTEGER',
        'reservation_data': 'TEXT',
        'refund_data': 'TEXT',
        'description': 'VARCHAR',
        'pack_id': 'INTEGER',
        'deletion_scheduled_at': 'TIMESTAMP WITH TIME ZONE' if not IS_SQLITE else 'DATETIME',
        'deletion_warning_sent_count': 'INTEGER DEFAULT 0 NOT NULL',
    }
    for col_name, col_type in invoice_additions.items():
        if col_name not in invoice_columns:
            with engine.connect() as conn:
                conn.execute(text(f"ALTER TABLE invoices ADD COLUMN {col_name} {col_type}"))
                conn.commit()

    # Coupon table migrations
    coupon_columns = [col['name'] for col in inspector.get_columns('coupons')]
    coupon_addition = {
        'applicable_to': 'VARCHAR DEFAULT \'all\'',
    }
    for col_name, col_type in coupon_addition.items():
        if col_name not in coupon_columns:
            with engine.connect() as conn:
                conn.execute(text(f"ALTER TABLE coupons ADD COLUMN {col_name} {col_type}"))
                conn.commit()

    # Reseller table migrations
    if 'resellers' in existing_tables:
        reseller_columns = [col['name'] for col in inspector.get_columns('resellers')]
        if 'inbound_ids' not in reseller_columns:
            with engine.connect() as conn:
                conn.execute(text("ALTER TABLE resellers ADD COLUMN inbound_ids TEXT"))
                conn.commit()

    # Plan table migrations (Amnezia service type)
    if 'plans' in existing_tables:
        plan_columns = [col['name'] for col in inspector.get_columns('plans')]
        if 'service_type' not in plan_columns:
            with engine.connect() as conn:
                conn.execute(text("ALTER TABLE plans ADD COLUMN service_type VARCHAR DEFAULT 'xui' NOT NULL"))
                conn.commit()

    # Invoice table migrations (Amnezia service link)
    if 'invoices' in existing_tables:
        invoice_col_names = [col['name'] for col in inspector.get_columns('invoices')]
        if 'amnezia_service_id' not in invoice_col_names:
            with engine.connect() as conn:
                conn.execute(text("ALTER TABLE invoices ADD COLUMN amnezia_service_id INTEGER"))
                conn.commit()

    # Referral events: when each invitee actually paid (window filtering)
    if 'referrals' in existing_tables:
        referral_cols = [col['name'] for col in inspector.get_columns('referrals')]
        if 'paid_at' not in referral_cols:
            paid_at_type = 'DATETIME' if IS_SQLITE else 'TIMESTAMPTZ'
            with engine.connect() as conn:
                conn.execute(text(f"ALTER TABLE referrals ADD COLUMN paid_at {paid_at_type}"))
                conn.commit()

    # Create reseller_packs table if it doesn't exist (handled by create_all, but ensure any missing columns)
    # The table will be created by Base.metadata.create_all, but we add a manual creation fallback.
    if 'reseller_packs' not in existing_tables:
        # The table will be created by create_all, but we can also explicitly create it here.
        pass  # Base.metadata.create_all will handle it

    # ========== Performance Indexes ==========
    # Add indexes to speed up common queries. These are safe and idempotent.
    def create_index_if_not_exists(table_name, index_name, columns, unique=False):
        """Create an index if it does not already exist."""
        existing_indexes = [idx['name'] for idx in inspector.get_indexes(table_name)]
        if index_name in existing_indexes:
            return
        unique_clause = "UNIQUE " if unique else ""
        stmt = f"CREATE {unique_clause}INDEX IF NOT EXISTS {index_name} ON {table_name} ({', '.join(columns)})"
        with engine.connect() as conn:
            conn.execute(text(stmt))
            conn.commit()

    # 1. invoices.status – for filtering pending/locked invoices (admin dashboard, retry)
    create_index_if_not_exists('invoices', 'idx_invoices_status', ['status'])

    # 2. invoices.created_at – for date-range billing reports
    create_index_if_not_exists('invoices', 'idx_invoices_created_at', ['created_at'])

    # 3. invoices.client_name – for quick lookups by email (user_owns_email, reseller_owns_email, service detail)
    create_index_if_not_exists('invoices', 'idx_invoices_client_name', ['client_name'])

    # 4. reseller_packs.expiry_date – for expiry checks and FIFO ordering in balance queries
    if 'reseller_packs' in existing_tables:
        create_index_if_not_exists('reseller_packs', 'idx_reseller_packs_expiry_date', ['expiry_date'])

    # 5. referrals.referrer_id – for counting paid referrals (referral info and reward claiming)
    if 'referrals' in existing_tables:
        create_index_if_not_exists('referrals', 'idx_referrals_referrer_id', ['referrer_id'])

    # 6. coupon_usage.coupon_id – for usage counting (though foreign key may already index, we add explicitly)
    if 'coupon_usage' in existing_tables:
        create_index_if_not_exists('coupon_usage', 'idx_coupon_usage_coupon_id', ['coupon_id'])

    # 7. invoices.amnezia_service_id – for renew/topup lookups
    create_index_if_not_exists('invoices', 'idx_invoices_amnezia_service_id', ['amnezia_service_id'])

    # 8. amnezia_services.telegram_user_id – for listing a user's Amnezia services
    if 'amnezia_services' in existing_tables:
        create_index_if_not_exists('amnezia_services', 'idx_amnezia_services_user', ['telegram_user_id'])
        create_index_if_not_exists('amnezia_services', 'idx_amnezia_services_status', ['status'])
        create_index_if_not_exists('amnezia_services', 'idx_amnezia_services_expiry_date', ['expiry_date'])
        amnezia_service_columns = [col['name'] for col in inspector.get_columns('amnezia_services')]
        for col_name, col_type in {
            'expiry_warned_at': 'TIMESTAMP WITH TIME ZONE' if not IS_SQLITE else 'DATETIME',
            'server_name': 'VARCHAR',
            'server_missing_notified_at': 'TIMESTAMP WITH TIME ZONE' if not IS_SQLITE else 'DATETIME',
            'panel_user_id': 'VARCHAR',
            'panel_username': 'VARCHAR',
            'panel_password': 'VARCHAR',
            # PostgreSQL rejects DEFAULT 0 on a boolean column — the literal
            # must be FALSE (this exact bug silently skipped the migration
            # on production Postgres while working fine under SQLite).
            'is_trial': 'BOOLEAN NOT NULL DEFAULT FALSE',
        }.items():
            if col_name not in amnezia_service_columns:
                with engine.connect() as conn:
                    conn.execute(text(f"ALTER TABLE amnezia_services ADD COLUMN {col_name} {col_type}"))
                    conn.commit()

    # 9. amnezia_users.panel_password – shown back to the user (panel web-UI login)
    if 'amnezia_users' in existing_tables:
        amnezia_user_columns = [col['name'] for col in inspector.get_columns('amnezia_users')]
        if 'panel_password' not in amnezia_user_columns:
            with engine.connect() as conn:
                conn.execute(text("ALTER TABLE amnezia_users ADD COLUMN panel_password VARCHAR"))
                conn.commit()

def init_db():
    db = SessionLocal()
    defaults = {
        'trial_traffic_gb': '0.1',
        'trial_duration_days': '1',
        'referral_threshold': '10',
        'referral_reward_plan_id': '1',
        'discount_percent': '5',
        'support_url': 'https://t.me/your_support',
        'shop_name': 'فروشگاه  رهانت',
        'max_receipt_size_mb': '10',
        'reseller_service_days': '30',
        'reseller_inbound_ids': ''
    }
    for key, value in defaults.items():
        if not db.query(AppSetting).filter(AppSetting.key == key).first():
            db.add(AppSetting(key=key, value=value))
    db.commit()
    db.close()

def migrate_legacy_to_packs():
    """One-time migration: convert remaining legacy traffic to packs with 99 days expiry."""
    from datetime import datetime, timedelta, timezone
    with SessionLocal() as db:
        # Check if migration already done
        done = db.query(AppSetting).filter(AppSetting.key == 'legacy_migration_done').first()
        if done:
            return
        # Find resellers with legacy traffic remaining
        resellers = db.query(Reseller).filter(
            Reseller.allowance_bytes > Reseller.used_bytes
        ).all()
        for reseller in resellers:
            remaining = reseller.allowance_bytes - reseller.used_bytes
            if remaining > 0:
                # Create a pack with 99 days expiry
                pack = ResellerPack(
                    reseller_id=reseller.telegram_user_id,
                    granted_bytes=remaining,
                    used_bytes=0,
                    expiry_date=datetime.now(timezone.utc) + timedelta(days=99),
                    is_active=True
                )
                db.add(pack)
                # Reset legacy fields
                reseller.allowance_bytes = 0
                reseller.used_bytes = 0
        # Mark migration as done
        db.add(AppSetting(key='legacy_migration_done', value='true'))
        db.commit()

def init_database():
    """Create tables, run migrations, seed defaults.
    Retries up to 30 seconds to handle concurrent multi-container startup.
    """
    for attempt in range(30):
        try:
            Base.metadata.create_all(bind=engine)
            run_migrations()
            init_db()
            migrate_legacy_to_packs()
            return
        except Exception as e:
            if attempt < 29:
                time.sleep(1)
            else:
                logger.warning(
                    "Database init failed after 30 attempts: %s. "
                    "Another container may have created the tables; will retry at first use.",
                    e
                )

if os.getenv('SKIP_DB_INIT', 'false').lower() != 'true':
    init_database()
