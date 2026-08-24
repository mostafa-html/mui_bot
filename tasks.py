import os
import time
import asyncio
import json
import logging
from datetime import datetime, timedelta, timezone
from celery import Celery
from celery.schedules import crontab
import redis.asyncio as redis
import httpx
from sqlalchemy import text
from database import SessionLocal, Invoice, Plan, TrialUsage, Referral, ReferralCode, AppSetting, Reseller, ResellerPack, AmneziaService, AmneziaTrial
from xui_client import XUIClient

from dotenv import load_dotenv
load_dotenv()

# Import shared utilities to avoid duplication
from src.utils.formatting import format_size
from src.services.reconcile import compute_reconcile, to_plan
from src.services.amnezia import AmneziaClient, AmneziaError, PLAY_STORE_URL, APP_STORE_URL
from src.utils.alerting import should_alert, is_noise, format_alert

# ---- Alert admins when a background task fails permanently ----
# Autoretried tasks only reach this on FINAL failure (intermediate retries
# emit task_retry instead), so a page here means real, unrecovered breakage.
def _admin_ids():
    """Admin Telegram IDs from env — same pattern used across this module.
    Was previously referenced but never defined, crashing the watchdog
    alerts and the daily digest with NameError."""
    return [int(x.strip()) for x in os.getenv('ADMIN_CHAT_IDS', '').split(',') if x.strip()]

# ---- Amnezia known-servers snapshot (for add/delete detection) ----
KNOWN_SERVERS_KEY = 'amnezia_known_servers'

async def _load_known_servers():
    """{server_id: name} from Redis; {} when unavailable."""
    url = os.getenv('REDIS_URL')
    if not url:
        return {}
    try:
        import redis.asyncio as aioredis
        r = aioredis.from_url(url, decode_responses=True)
        try:
            raw = await r.hgetall(KNOWN_SERVERS_KEY)
            return {int(k): v for k, v in (raw or {}).items()}
        finally:
            await r.aclose()
    except Exception as e:
        logging.debug(f"known-servers load skipped: {e}")
        return {}

async def _save_known_servers(mapping):
    try:
        url = os.getenv('REDIS_URL')
        if not url:
            return
        import redis.asyncio as aioredis
        r = aioredis.from_url(url, decode_responses=True)
        try:
            await r.delete(KNOWN_SERVERS_KEY)
            if mapping:
                await r.hset(KNOWN_SERVERS_KEY,
                             mapping={str(k): str(v) for k, v in mapping.items()})
        finally:
            await r.aclose()
    except Exception as e:
        logging.debug(f"known-servers save skipped: {e}")

def _pick_migration_target(servers, alive_by_id, added_ids, exclude_id=None):
    """Newly-added alive server first; else any alive server != excluded."""
    candidates = [s['id'] for s in servers if s.get('alive', True)]
    preferred = [i for i in added_ids if i in candidates and i != exclude_id]
    if preferred:
        return preferred[0]
    others = [i for i in candidates if i != exclude_id]
    return others[0] if others else None

from celery.signals import task_failure

@task_failure.connect
def alert_on_task_failure(sender=None, task_id=None, exception=None,
                          args=None, kwargs=None, **kw):
    try:
        if exception is None or is_noise(exception):
            return
        task_name = getattr(sender, 'name', str(sender)) or 'unknown-task'
        sig = f"{type(exception).__name__}:{str(exception)[:150]}"
        alert_now, suppressed = should_alert(f"worker:{sig}")
        if not alert_now:
            return
        ctx = (f"وظیفه <code>{task_name}</code> | id <code>{task_id}</code>\n"
               f"args: <code>{str(args)[:150]}</code>")
        alert = format_alert("وظیفه پس‌زمینه", exception, context=ctx,
                             suppressed=suppressed)

        async def _send():
            admin_ids = [int(x.strip()) for x in os.getenv('ADMIN_CHAT_IDS', '').split(',') if x.strip()]
            await notify_many_users([(aid, alert) for aid in admin_ids])
        run_async(_send())
    except Exception:
        logging.error("failed to deliver task-failure alert", exc_info=True)

REDIS_URL = os.getenv('REDIS_URL')

async def edit_telegram_message(chat_id, message_id, text):
    bot_token = os.getenv('BOT_TOKEN')
    if not bot_token:
        return
    url = f"https://api.telegram.org/bot{bot_token}/editMessageText"
    async with httpx.AsyncClient() as client:
        await client.post(url, json={"chat_id": chat_id, "message_id": message_id, "text": text, "parse_mode": "HTML"})

async def update_loading_message(invoice_id, text):
    r = redis.Redis.from_url(REDIS_URL, decode_responses=True)
    key = f"loading:{invoice_id}"
    val = await r.get(key)
    if val:
        try:
            chat_id, msg_id = val.split(':')
            await edit_telegram_message(chat_id, int(msg_id), text)
        except Exception:
            pass
        await r.delete(key)

async def animate_loading_message(invoice_id, frames, final_text=None):
    """Animate a loading message with a series of frames, then optionally set final text."""
    r = redis.Redis.from_url(REDIS_URL, decode_responses=True)
    key = f"loading:{invoice_id}"
    val = await r.get(key)
    if val:
        try:
            chat_id, msg_id = val.split(':')
            chat_id = int(chat_id)
            msg_id = int(msg_id)
            for frame in frames:
                await edit_telegram_message(chat_id, msg_id, frame)
                await asyncio.sleep(0.3)
            if final_text:
                await edit_telegram_message(chat_id, msg_id, final_text)
        except Exception:
            pass
        finally:
            await r.delete(key)

celery_app = Celery('tasks', broker=REDIS_URL)

celery_app.conf.beat_schedule = {
    'daily_backup_to_tg': {
        'task': 'tasks.trigger_daily_backup',
        'schedule': crontab(hour=3, minute=0),
    },
    'check_low_traffic_and_expiry': {
        'task': 'tasks.alert_low_traffic_expiry',
        'schedule': crontab(minute=0, hour='8,20'),
    },
    'check_stalled_invoices': {
        'task': 'tasks.check_stalled_invoices',
        'schedule': crontab(minute='*/15'),
    },
    'check_reseller_pack_expiry': {
        'task': 'tasks.check_reseller_pack_expiry',
        'schedule': crontab(hour=2, minute=0),
    },
    'collect_panel_traffic': {
        'task': 'tasks.collect_panel_traffic',
        'schedule': crontab(minute=55, hour=23),
    },
    'check_expired_services_for_deletion': {
        'task': 'tasks.check_expired_services_for_deletion',
        'schedule': crontab(hour=10, minute=0),  # Daily at 10 AM
    },
    'check_amnezia_expiry': {
        'task': 'tasks.check_amnezia_expiry',
        'schedule': crontab(hour=9, minute=30),
    },
    'check_amnezia_servers': {
        'task': 'tasks.check_amnezia_servers',
        'schedule': crontab(minute=5),  # hourly
    },
    'system_heartbeat': {
        'task': 'tasks.system_heartbeat',
        'schedule': crontab(minute='*/10'),  # dead-man's-switch checker
    },
    'daily_status_digest': {
        'task': 'tasks.daily_status_digest',
        'schedule': crontab(hour=21, minute=0),
    },
}

# Single event loop per task — all coroutines grouped together
def run_async(coro):
    return asyncio.run(coro)

async def invalidate_cache(tg_id: int):
    """Clear the cached email list for a user.
    Creates a fresh Redis connection (no global pool) because each
    Celery task runs in its own event loop via asyncio.run().
    """
    r = redis.Redis.from_url(REDIS_URL, decode_responses=True)
    try:
        await r.delete(f"user_emails:{tg_id}")
    finally:
        await r.aclose()

async def notify_user(tg_id: int, text: str, effect: str = None):
    bot_token = os.getenv('BOT_TOKEN')
    if bot_token:
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        payload = {"chat_id": tg_id, "text": text, "parse_mode": "HTML"}
        # Message effects (Bot API 7.4+) work in private chats for ALL users.
        # An invalid id makes Telegram reject just this send, which the raw
        # POST ignores — so a bad constant can never break a notification.
        if effect:
            payload["message_effect_id"] = effect
        async with httpx.AsyncClient() as client:
            await client.post(url, json=payload)

async def notify_many_users(notifications: list):
    """Send multiple notifications in a single event loop with rate limiting.

    Each entry is ``(tg_id, text)`` or ``(tg_id, text, effect_id)``."""
    bot_token = os.getenv('BOT_TOKEN')
    if not bot_token:
        return
    async with httpx.AsyncClient() as client:
        for i, item in enumerate(notifications):
            tg_id, text = item[0], item[1]
            url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
            payload = {"chat_id": tg_id, "text": text, "parse_mode": "HTML"}
            if len(item) > 2 and item[2]:
                payload["message_effect_id"] = item[2]
            try:
                await client.post(url, json=payload)
            except Exception:
                pass
            # Rate limiting: ~25 msg/sec (stay under Telegram's 30/sec limit)
            if i > 0 and i % 20 == 0:
                await asyncio.sleep(1)

# Message-effect ids — LIVE-VERIFIED against the Bot API (2026-08-22) and
# confirmed FREE (no Telegram Premium required on the recipient). Do NOT swap
# in prettier effects without re-testing: many are premium-gated for the
# recipient (PREMIUM_ACCOUNT_REQUIRED) and full id list:
# https://gist.github.com/wiz0u/2a6d40c8f635687be363d72251a264da
MSG_EFFECT_CONFETTI = "5046509860389126442"   # 🎉 confetti — new service delivered
MSG_EFFECT_FIRE = "5104841245755180586"       # 🔥 fire — renewal done

@celery_app.task
def broadcast_message(admin_id: int, text: str):
    """Send one message to every bot user, rate-limited, then report sent/failed to the admin.
    A failure (e.g. user blocked the bot) is counted, not retried."""
    async def _run():
        bot_token = os.getenv('BOT_TOKEN')
        if not bot_token:
            return
        # Recipients: everyone with a ReferralCode (created on /start), unioned with invoice users
        with SessionLocal() as db:
            ids = {row[0] for row in db.query(ReferralCode.telegram_user_id).all()}
            ids.update(row[0] for row in db.query(Invoice.telegram_user_id).distinct().all())
        recipients = [i for i in ids if i]

        sent = 0
        failed = 0
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        async with httpx.AsyncClient(timeout=15.0) as client:
            for i, tg_id in enumerate(recipients):
                try:
                    resp = await client.post(url, json={"chat_id": tg_id, "text": text, "parse_mode": "HTML"})
                    if resp.status_code == 200 and resp.json().get('ok'):
                        sent += 1
                    else:
                        failed += 1  # blocked bot / deactivated / chat not found
                except Exception:
                    failed += 1
                # Rate limiting: ~25 msg/sec (stay under Telegram's 30/sec limit)
                if i > 0 and i % 20 == 0:
                    await asyncio.sleep(1)

        report = (
            f"📢 <b>گزارش پیام همگانی</b>\n"
            f"━━━━━━━━━━━━━━\n"
            f"👥 کل گیرندگان: <b>{len(recipients)}</b>\n"
            f"✅ ارسال موفق: <b>{sent}</b>\n"
            f"❌ ناموفق (مسدود/غیرفعال): <b>{failed}</b>"
        )
        await notify_user(admin_id, report)
    run_async(_run())

async def get_sub_link(email: str) -> str:
    xui = XUIClient()
    full = await xui.get_client_full(email)
    if full and 'client' in full:
        sub_id = full['client'].get('subId')
        if sub_id:
            with SessionLocal() as db:
                setting = db.query(AppSetting).filter(AppSetting.key == 'sub_base_link').first()
                base = setting.value if setting else None
            if base:
                if not base.endswith('/'): base += '/'
                return base + sub_id
    return None

async def notify_admins(text: str):
    admin_ids = [int(x.strip()) for x in os.getenv('ADMIN_CHAT_IDS', '').split(',') if x.strip()]
    await notify_many_users([(aid, text) for aid in admin_ids])

async def send_admin_document(filename: str, content: bytes, caption: str):
    bot_token = os.getenv('BOT_TOKEN')
    if not bot_token:
        return
    admin_ids = [int(x.strip()) for x in os.getenv('ADMIN_CHAT_IDS', '').split(',') if x.strip()]
    if not admin_ids:
        return
    url = f"https://api.telegram.org/bot{bot_token}/sendDocument"
    async with httpx.AsyncClient(timeout=60.0) as client:
        for admin_id in admin_ids:
            files = {"document": (filename, content, "application/octet-stream")}
            data = {"chat_id": str(admin_id), "caption": caption, "parse_mode": "HTML"}
            try:
                await client.post(url, data=data, files=files)
            except Exception:
                pass

def build_bot_db_backup() -> tuple[str, bytes]:
    database_url = os.getenv('DATABASE_URL', 'sqlite:///storage/bot.db')
    timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
    if database_url.startswith('sqlite:///'):
        sqlite_path = database_url.replace('sqlite:///', '', 1)
        with open(sqlite_path, 'rb') as handle:
            return f"botdb_{timestamp}.db", handle.read()

    with SessionLocal() as db:
        lines = [
            f"-- vpnBot logical backup generated at {datetime.utcnow().isoformat()}Z",
            f"-- source database: {database_url}",
            "",
        ]
        table_names = [row[0] for row in db.execute(text(
            "SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = 'public' ORDER BY tablename"
        ))]
        for table_name in table_names:
            lines.append(f"-- TABLE {table_name}")
            rows = db.execute(text(f'SELECT row_to_json(t) FROM (SELECT * FROM \"{table_name}\") t'))
            for row in rows:
                lines.append(str(row[0]))
            lines.append("")
        payload = "\n".join(lines).encode('utf-8')
        return f"botdb_{timestamp}.jsonl", payload

# ----- Helper: mark referral paid and notify referrer -----
def mark_referral_paid(user_id: int):
    """Mark referral as paid; returns notification info to be sent later in the same event loop."""
    with SessionLocal() as db:
        referral = db.query(Referral).filter(Referral.referred_user_id == user_id).first()
        if referral and not referral.became_paid:
            referral.became_paid = True
            db.commit()
            paid_count = db.query(Referral).filter(
                Referral.referrer_id == referral.referrer_id,
                Referral.became_paid == True
            ).count()
            threshold_setting = db.query(AppSetting).filter(AppSetting.key == 'referral_threshold').first()
            threshold = int(threshold_setting.value) if threshold_setting else 10
            if paid_count >= threshold:
                return [(referral.referrer_id,
                    f"🎉 <b>تبریک! یک نفر دیگر از طریق لینک شما سرویس خرید!</b>\n\n"
                    f"اکنون <b>{paid_count}</b> دعوت موفق دارید و به آستانه {threshold} رسیده‌اید!\n"
                    f"از منوی «🤝 دعوت از دوستان» جایزه خود را دریافت کنید.")]
            else:
                return [(referral.referrer_id,
                    f"🎉 <b>یک نفر از طریق لینک شما سرویس خرید!</b>\n\n"
                    f"تعداد دعوت‌های موفق شما: <b>{paid_count}</b> از {threshold}")]
    return []

# ----- New tasks -----
@celery_app.task
def provision_trial(user_id: int, client_name: str, traffic_gb, days: int):
    async def _run():
        email = f"trial_{user_id}_{int(time.time())}"
        total_bytes = int(traffic_gb * 1024 ** 3)  # int64 for panel API (rejects floats)
        expiry_ms = int((time.time() + (days * 86400)) * 1000)
        xui = XUIClient()
        inbounds = await xui.get_enabled_inbounds()
        inbound_ids = [ib['id'] for ib in inbounds]
        await xui.add_client(email, total_bytes, expiry_ms, inbound_ids)
        await xui.assign_group(email, str(user_id))
        await invalidate_cache(user_id)
        
        with SessionLocal() as db:
            trial = TrialUsage(telegram_user_id=user_id, last_trial_date=datetime.now(timezone.utc), service_email=email)
            db.add(trial)
            invoice = Invoice(
                telegram_user_id=user_id,
                plan_id=None,
                total_price=0,
                original_price=0,
                discount_amount=0,
                client_name=email,
                action_type="TRIAL",
                status="COMPLETE"
            )
            db.add(invoice)
            db.commit()
        
        await notify_user(user_id, f"🎁 <b>تست رایگان شما فعال شد!</b>\n\n📦 حجم: {traffic_gb} GB\n⏳ مدت: {days} روز\n\nلطفاً از منوی «مدیریت سرویس‌های من» لینک اتصال را دریافت کنید.")
    run_async(_run())

@celery_app.task
def provide_referral_reward(user_id: int, plan_id: int):
    async def _run():
        with SessionLocal() as db:
            plan = db.query(Plan).filter(Plan.id == plan_id).first()
            if not plan:
                return
        email = f"reward_{user_id}_{int(time.time())}"
        total_bytes = plan.traffic_gb * 1024 ** 3
        expiry_ms = int((time.time() + (plan.duration_days * 86400)) * 1000)
        xui = XUIClient()
        inbounds = await xui.get_enabled_inbounds()
        inbound_ids = [ib['id'] for ib in inbounds]
        await xui.add_client(email, total_bytes, expiry_ms, inbound_ids)
        await xui.assign_group(email, str(user_id))
        await invalidate_cache(user_id)
        
        with SessionLocal() as db:
            invoice = Invoice(
                telegram_user_id=user_id,
                plan_id=plan_id,
                total_price=0,
                original_price=0,
                discount_amount=0,
                client_name=email,
                action_type="REFERRAL_REWARD",
                status="COMPLETE"
            )
            db.add(invoice)
            db.commit()
        
        await notify_user(user_id, f"🎉 <b>جایزه معرفی شما فعال شد!</b>\nپلن {plan.name} به سرویس‌های شما اضافه شد.")
    run_async(_run())

# ----- Reseller provisioning (allowance-funded, refund on failure, no autoretry) -----
def _refund_reseller(invoice_id: int):
    """Refund reserved allowance using reservation_data from the invoice and mark it FAILED.
    Only handles pack-based reservations (legacy is deprecated)."""
    with SessionLocal() as db:
        inv = db.query(Invoice).filter(Invoice.id == invoice_id).first()
        if not inv:
            return
        reservation_data = inv.reservation_data
        if reservation_data:
            data = json.loads(reservation_data)
            # Reverse pack deductions
            for pack_info in data.get('packs', []):
                pack_id = pack_info['id']
                deducted = pack_info['deducted']
                db.query(ResellerPack).filter(ResellerPack.id == pack_id).update(
                    {ResellerPack.used_bytes: ResellerPack.used_bytes - deducted},
                    synchronize_session=False
                )
                # Re-activate pack if it was fully used
                pack = db.query(ResellerPack).filter(ResellerPack.id == pack_id).first()
                if pack and pack.used_bytes < pack.granted_bytes:
                    pack.is_active = True
        inv.status = "FAILED"
        db.commit()

def _refund_reseller_services(email: str, reseller_id: int):
    """Reverse all reservations for a given email owned by reseller_id.
    Used when deletion fails to refund the reseller's allowance.
    """
    with SessionLocal() as db:
        invoices = db.query(Invoice).filter(
            Invoice.reseller_id == reseller_id,
            Invoice.client_name == email,
            Invoice.status.in_(['COMPLETE', 'PROCESSING'])
        ).with_for_update().all()
        if not invoices:
            return
        # Aggregate reservations
        agg_packs = {}
        total_legacy = 0
        for inv in invoices:
            if inv.reservation_data:
                data = json.loads(inv.reservation_data)
                for pack_info in data.get('packs', []):
                    pack_id = pack_info['id']
                    agg_packs[pack_id] = agg_packs.get(pack_id, 0) + pack_info['deducted']
                total_legacy += data.get('legacy', 0)
        # Reverse pack deductions
        for pack_id, deducted in agg_packs.items():
            pack = db.query(ResellerPack).filter(ResellerPack.id == pack_id).with_for_update().first()
            if pack:
                pack.used_bytes = max(0, pack.used_bytes - deducted)
                if pack.used_bytes < pack.granted_bytes:
                    pack.is_active = True
        # Reverse legacy
        if total_legacy > 0:
            reseller = db.query(Reseller).filter(Reseller.telegram_user_id == reseller_id).with_for_update().first()
            if reseller:
                reseller.used_bytes = max(0, reseller.used_bytes - total_legacy)
        # Mark invoices as FAILED
        for inv in invoices:
            inv.status = "FAILED"
        db.commit()

def _reseller_inbound_ids(db, xui_inbounds: list, reseller_id: int = None) -> list:
    """Resolve reseller inbounds exclusively from the global setting.

    ``reseller_id`` remains in the signature for compatibility with existing
    callers. Historical per-reseller values are intentionally ignored.
    """
    setting = db.query(AppSetting).filter(AppSetting.key == 'reseller_inbound_ids').first()
    raw = (setting.value if setting else '') or ''
    ids = [int(x) for x in raw.split(',') if x.strip().isdigit()]
    if not ids:
        return [ib['id'] for ib in xui_inbounds]
    valid = {ib['id'] for ib in xui_inbounds}
    filtered = [i for i in ids if i in valid]
    if len(filtered) != len(ids):
        logging.warning(f"Global reseller_inbound_ids contains invalid IDs: {ids}")
    # Fallback to all inbounds if none are valid
    if not filtered:
        logging.warning("No valid inbound IDs found; falling back to all inbounds.")
        return [ib['id'] for ib in xui_inbounds]
    return filtered

@celery_app.task(bind=True, autoretry_for=(Exception,), retry_backoff=True, max_retries=5)
def provision_reseller_new(self, invoice_id: int):
    async def _run():
        with SessionLocal() as db:
            invoice = db.query(Invoice).filter(Invoice.id == invoice_id).first()
            if not invoice:
                return
            reseller_id = invoice.reseller_id
            gb = invoice.added_gb or 0
            base_name = invoice.client_name
            days_setting = db.query(AppSetting).filter(AppSetting.key == 'reseller_service_days').first()
            days = int(days_setting.value) if days_setting else 30
        try:
            email = f"{base_name}_{int(time.time())}"
            total_bytes = gb * 1024 ** 3
            expiry_ms = int((time.time() + (days * 86400)) * 1000)
            xui = XUIClient()
            inbounds = await xui.get_enabled_inbounds()
            with SessionLocal() as db:
                inbound_ids = _reseller_inbound_ids(db, inbounds, reseller_id)
            await xui.add_client(email, total_bytes, expiry_ms, inbound_ids)
            await xui.assign_group(email, str(reseller_id))
            await invalidate_cache(reseller_id)
            with SessionLocal() as db:
                inv = db.query(Invoice).filter(Invoice.id == invoice_id).first()
                inv.status = "COMPLETE"
                inv.client_name = email
                db.commit()
            sub_link = await get_sub_link(email)
            link_text = f"🔗 <b>لینک اتصال:</b>\n<code>{sub_link}</code>" if sub_link else "⚠️ لینک اتصال هنوز تنظیم نشده است. لطفاً به منوی سرویس‌های من مراجعه کنید."
            await notify_user(reseller_id, f"✅ <b>سرویس نمایندگی ساخته شد!</b>\n\n نام سرویس: <code>{email}</code>\n📦 حجم: {gb} GB\n⏳ مدت: {days} روز\n\n{link_text}")
        except Exception as e:
            if self.request.retries >= self.max_retries:
                _refund_reseller(invoice_id)
                await notify_user(reseller_id, "❌ <b>ساخت سرویس نمایندگی ناموفق بود.</b>\n\nموجودی ترافیک شما بازگردانده شد. لطفاً دوباره تلاش کنید یا به مدیریت اطلاع دهید.")
                logging.error(f"Reseller new provisioning failed after {self.max_retries} retries: {e}")
            else:
                logging.warning(f"Reseller new provisioning attempt {self.request.retries+1} failed: {e}")
            raise
    run_async(_run())

@celery_app.task(bind=True, autoretry_for=(Exception,), retry_backoff=True, max_retries=5)
def provision_reseller_topup(self, invoice_id: int):
    async def _run():
        with SessionLocal() as db:
            invoice = db.query(Invoice).filter(Invoice.id == invoice_id).first()
            if not invoice:
                return
            reseller_id = invoice.reseller_id
            gb = invoice.added_gb or 0
            email = invoice.client_name
        try:
            xui = XUIClient()
            # No need to call _reseller_inbound_ids for topup because we only update existing client
            full_client_response = await xui.get_client_full(email)
            client_payload = full_client_response.get('client', {})
            new_total_bytes = client_payload.get('totalGB', 0) + (gb * 1024 ** 3)
            update_payload = {
                "email": email,
                "totalGB": new_total_bytes,
                "expiryTime": client_payload.get('expiryTime', 0),
                "tgId": client_payload.get('tgId', 0),
                "enable": True
            }
            await xui.update_client(email, update_payload)
            await invalidate_cache(reseller_id)
            with SessionLocal() as db:
                inv = db.query(Invoice).filter(Invoice.id == invoice_id).first()
                inv.status = "COMPLETE"
                db.commit()
            await notify_user(reseller_id, f"➕ <b>{gb} گیگابایت به <code>{email}</code> اضافه شد.</b>")
        except Exception as e:
            if self.request.retries >= self.max_retries:
                _refund_reseller(invoice_id)
                await notify_user(reseller_id, "❌ <b>افزودن حجم ناموفق بود.</b>\n\nموجودی ترافیک شما بازگردانده شد.")
                logging.error(f"Reseller topup failed after {self.max_retries} retries: {e}")
            else:
                logging.warning(f"Reseller topup attempt {self.request.retries+1} failed: {e}")
            raise
    run_async(_run())

@celery_app.task(bind=True, autoretry_for=(Exception,), retry_backoff=True, max_retries=5)
def provision_reseller_renew(self, invoice_id: int):
    async def _run():
        with SessionLocal() as db:
            invoice = db.query(Invoice).filter(Invoice.id == invoice_id).first()
            if not invoice:
                return
            reseller_id = invoice.reseller_id
            gb = invoice.added_gb or 0
            email = invoice.client_name
            days_setting = db.query(AppSetting).filter(AppSetting.key == 'reseller_service_days').first()
            days = int(days_setting.value) if days_setting else 30
        try:
            xui = XUIClient()
            # No need to call _reseller_inbound_ids for renew because we only update existing client
            full_client_response = await xui.get_client_full(email)
            client_payload = full_client_response.get('client', {})
            now_ms = int(time.time() * 1000)
            used = full_client_response.get('usedTraffic', 0)
            remaining_bytes = max(0, client_payload.get('totalGB', 0) - used)
            remaining_days_ms = max(0, client_payload.get('expiryTime', 0) - now_ms)
            new_total_bytes = remaining_bytes + (gb * 1024 ** 3)
            new_expiry_ms = now_ms + remaining_days_ms + (days * 86400 * 1000)
            update_payload = {
                "email": email,
                "totalGB": new_total_bytes,
                "expiryTime": new_expiry_ms,
                "tgId": client_payload.get('tgId', 0),
                "enable": True
            }
            await xui.update_client(email, update_payload)
            await xui.reset_client_traffic(email)
            await invalidate_cache(reseller_id)
            with SessionLocal() as db:
                inv = db.query(Invoice).filter(Invoice.id == invoice_id).first()
                inv.status = "COMPLETE"
                db.commit()
            await notify_user(reseller_id, f"🔄 <b>سرویس <code>{email}</code> تمدید شد.</b>\n\n📦 +{gb} GB | ⏳ +{days} روز")
        except Exception as e:
            if self.request.retries >= self.max_retries:
                _refund_reseller(invoice_id)
                await notify_user(reseller_id, "❌ <b>تمدید ناموفق بود.</b>\n\nموجودی ترافیک شما بازگردانده شد.")
                logging.error(f"Reseller renew failed after {self.max_retries} retries: {e}")
            else:
                logging.warning(f"Reseller renew attempt {self.request.retries+1} failed: {e}")
            raise
    run_async(_run())

# ----- Reseller traffic pack purchase -----
@celery_app.task(bind=True, autoretry_for=(Exception,), retry_backoff=True, max_retries=3)
def add_reseller_pack(self, invoice_id: int):
    async def _run():
        with SessionLocal() as db:
            invoice = db.query(Invoice).filter(Invoice.id == invoice_id).first()
            if not invoice or invoice.status != "PENDING":
                return
            pack = db.query(TrafficPack).filter(TrafficPack.id == invoice.pack_id).first()
            if not pack:
                invoice.status = "FAILED"
                db.commit()
                await notify_user(invoice.telegram_user_id, "❌ <b>بسته ترافیک یافت نشد. خرید لغو شد.</b>")
                return
            reseller_id = invoice.reseller_id
            if not reseller_id:
                invoice.status = "FAILED"
                db.commit()
                return
            expiry = datetime.now(timezone.utc) + timedelta(days=pack.duration_days)
            reseller_pack = ResellerPack(
                reseller_id=reseller_id,
                granted_bytes=pack.traffic_gb * 1024**3,
                used_bytes=0,
                expiry_date=expiry,
                is_active=True
            )
            db.add(reseller_pack)
            invoice.status = "COMPLETE"
            db.commit()
            await notify_user(reseller_id,
                f"✅ <b>بسته ترافیک {pack.name} خریداری شد!</b>\n\n"
                f"📦 حجم: {pack.traffic_gb} GB\n"
                f"⏳ مدت: {pack.duration_days} روز\n"
                f"📅 انقضا: {expiry.strftime('%Y-%m-%d')}\n\n"
                f"اکنون می‌توانید از این ترافیک برای ساخت سرویس‌های نمایندگی استفاده کنید."
            )
    run_async(_run())

# ----- Reseller service deletion -----
@celery_app.task(bind=True, autoretry_for=(Exception,), retry_backoff=True, max_retries=3)
def delete_reseller_service(self, invoice_id: int):
    """Delete a reseller-created service and refund unused traffic to the reseller's allowance.
    Aggregates all invoices for the same email, refunds proportionally to packs/legacy,
    disables the client on the panel, and marks all invoices as DELETED.
    On final failure, refunds all reservations and marks invoices as FAILED."""
    async def _run():
        # First, fetch the email and reseller_id from the trigger invoice (outside the main try)
        with SessionLocal() as db:
            trigger_invoice = db.query(Invoice).filter(Invoice.id == invoice_id).first()
            if not trigger_invoice:
                return
            email = trigger_invoice.client_name
            reseller_id = trigger_invoice.reseller_id
            if not email or not reseller_id:
                return

        try:
            with SessionLocal() as db:
                # 1) Lock the reseller row
                reseller = db.query(Reseller).filter(Reseller.telegram_user_id == reseller_id).with_for_update().first()
                if not reseller or not reseller.is_active:
                    return

                # 2) Find all active invoices for this email owned by this reseller
                invoices = db.query(Invoice).filter(
                    Invoice.reseller_id == reseller_id,
                    Invoice.client_name == email,
                    Invoice.status.in_(['COMPLETE', 'PROCESSING'])
                ).with_for_update().all()
                if not invoices:
                    return

                # 3) Aggregate reservation data from all invoices
                agg_packs = {}  # pack_id -> total deducted
                total_legacy = 0
                valid_invoices = []
                for inv in invoices:
                    if inv.reservation_data:
                        data = json.loads(inv.reservation_data)
                        for pack_info in data.get('packs', []):
                            pack_id = pack_info['id']
                            agg_packs[pack_id] = agg_packs.get(pack_id, 0) + pack_info['deducted']
                        total_legacy += data.get('legacy', 0)
                        valid_invoices.append(inv)
                if not valid_invoices:
                    # No reservation data → nothing to refund
                    # Still mark invoices as DELETED and disable client
                    for inv in invoices:
                        inv.status = "DELETED"
                    db.commit()
                    await notify_user(reseller_id, f"⚠️ <b>سرویس <code>{email}</code> حذف شد.</b>\n\nهیچ ترافیکی برای بازگشت وجود نداشت.")
                    return

                grand_total_reserved = sum(agg_packs.values()) + total_legacy
                if grand_total_reserved <= 0:
                    for inv in invoices:
                        inv.status = "DELETED"
                    db.commit()
                    await notify_user(reseller_id, f"⚠️ <b>سرویس <code>{email}</code> حذف شد.</b>\n\nهیچ ترافیکی برای بازگشت وجود نداشت.")
                    return

                # 4) Get current client stats from panel
                xui = XUIClient()
                try:
                    full_client = await xui.get_client_full(email)
                except Exception as e:
                    if "record not found" in str(e).lower():
                        # Client not found on panel — treat as no refund, just mark as deleted
                        for inv in invoices:
                            inv.status = "DELETED"
                        db.commit()
                        await notify_user(reseller_id, f"⚠️ <b>سرویس <code>{email}</code> در پنل یافت نشد.</b>\n\nحذف شد اما هیچ ترافیکی بازگردانده نشد.")
                        return
                    else:
                        raise  # re-raise other exceptions
                if not full_client or 'client' not in full_client:
                    # Client not found on panel — treat as no refund, just disable (or mark as deleted)
                    for inv in invoices:
                        inv.status = "DELETED"
                    db.commit()
                    await notify_user(reseller_id, f"⚠️ <b>سرویس <code>{email}</code> در پنل یافت نشد.</b>\n\nحذف شد اما هیچ ترافیکی بازگردانده نشد.")
                    return

                client_data = full_client['client']
                total_gb = client_data.get('totalGB', 0)
                used_traffic = full_client.get('usedTraffic', 0)
                expiry_ms = client_data.get('expiryTime', 0)
                now_ms = int(time.time() * 1000)

                # 5) If expired, no refund
                if expiry_ms > 0 and now_ms > expiry_ms:
                    unused_bytes = 0
                else:
                    unused_bytes = max(0, total_gb - used_traffic)

                refund_details = {
                    'total_reserved': grand_total_reserved,
                    'unused_refunded': unused_bytes,
                    'packs': {},
                    'legacy_refund': 0,
                    'expired': expiry_ms > 0 and now_ms > expiry_ms
                }

                # 6) Refund proportionally if unused > 0
                if unused_bytes > 0:
                    # Refund to packs
                    for pack_id, deducted in agg_packs.items():
                        proportion = deducted / grand_total_reserved
                        refund = int(proportion * unused_bytes)
                        if refund <= 0:
                            continue
                        pack = db.query(ResellerPack).filter(ResellerPack.id == pack_id).with_for_update().first()
                        if pack and pack.is_active:
                            # Ensure we don't refund more than used
                            actual_used = pack.used_bytes
                            refund_actual = min(refund, actual_used)
                            if refund_actual > 0:
                                pack.used_bytes -= refund_actual
                                if pack.used_bytes < pack.granted_bytes:
                                    pack.is_active = True
                                refund_details['packs'][pack_id] = refund_actual

                    # Refund to legacy
                    if total_legacy > 0:
                        proportion = total_legacy / grand_total_reserved
                        refund = int(proportion * unused_bytes)
                        if refund > 0:
                            actual_used = reseller.used_bytes
                            refund_actual = min(refund, actual_used)
                            if refund_actual > 0:
                                reseller.used_bytes -= refund_actual
                                refund_details['legacy_refund'] = refund_actual

                # 7) Disable client on panel — send full payload to avoid "client email is required" error
                update_payload = {
                    "email": email,
                    "totalGB": client_data.get('totalGB', 0),
                    "expiryTime": client_data.get('expiryTime', 0),
                    "tgId": client_data.get('tgId', 0),
                    "enable": False
                }
                await xui.update_client(email, update_payload)

                # 8) Mark all invoices as DELETED and store refund_data
                for inv in valid_invoices:
                    inv.status = "DELETED"
                    inv.refund_data = refund_details

                # Also mark any other invoices (if any) as DELETED without refund_data (they had no reservation)
                for inv in invoices:
                    if inv not in valid_invoices:
                        inv.status = "DELETED"

                db.commit()

            # 9) Notify reseller
            if unused_bytes > 0:
                refund_text = f"📤 ترافیک بازگردانده‌شده: <b>{format_size(unused_bytes)}</b>"
            else:
                refund_text = "⚠️ سرویس منقضی شده بود، هیچ ترافیکی بازگردانده نشد."
            await notify_user(reseller_id, 
                f"🗑 <b>سرویس <code>{email}</code> حذف شد.</b>\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"{refund_text}\n"
                f"✅ سرویس غیرفعال شد و تمام فاکتورهای مربوطه بسته شدند."
            )

        except Exception as e:
            # On failure, if retries are exhausted, refund the reseller and mark invoices as FAILED
            if self.request.retries >= self.max_retries:
                logging.error(f"Reseller deletion failed after {self.max_retries} retries: {e}")
                # Refund all reservations for this email
                _refund_reseller_services(email, reseller_id)
                await notify_user(reseller_id, 
                    f"❌ <b>حذف سرویس <code>{email}</code> ناموفق بود.</b>\n\n"
                    f"ترافیک شما بازگردانده شد. لطفاً دوباره تلاش کنید یا به مدیریت اطلاع دهید."
                )
                # Re-raise to mark the task as failed (but we've already handled the refund)
                raise
            else:
                logging.warning(f"Reseller deletion attempt {self.request.retries+1} failed: {e}")
                raise

    run_async(_run())

# ----- Existing provisioning tasks (with referral fix) -----
@celery_app.task(bind=True, autoretry_for=(Exception,), retry_backoff=True, max_retries=5)
def provision_new(self, invoice_id: int, inbound_ids: list):
    async def _run():
        try:
            # Fetch invoice and plan, extract all needed data while session is active
            with SessionLocal() as db:
                invoice = db.query(Invoice).filter(Invoice.id == invoice_id).first()
                if not invoice:
                    return
                # If already complete, skip
                if invoice.status == "COMPLETE":
                    return
                plan = db.query(Plan).filter(Plan.id == invoice.plan_id).first()
                if not plan:
                    invoice.status = "FAILED"
                    db.commit()
                    await notify_user(invoice.telegram_user_id, "❌ پلن مربوطه یافت نشد. لطفاً با پشتیبانی تماس بگیرید.")
                    return

                # Store all needed data before session closes
                user_id = invoice.telegram_user_id
                plan_name = plan.name
                traffic_gb = plan.traffic_gb
                duration_days = plan.duration_days
                original_name = invoice.client_name
                # Idempotent: a redo of a stuck invoice reads the already-updated
                # client_name (line ~824 writes final_email back). Appending the
                # suffix again would build Alikargar_528_528_... and create a fresh
                # orphan panel client on every redo. Only add the suffix once.
                suffix = f"_{invoice_id}"
                final_email = original_name if original_name.endswith(suffix) else f"{original_name}{suffix}"

            # XUI operations
            xui = XUIClient()
            existing = await xui.get_client_full(final_email)
            client_exists = existing and 'client' in existing

            if not client_exists:
                total_bytes = traffic_gb * 1024 ** 3
                expiry_ms = int((time.time() + (duration_days * 86400)) * 1000)
                await xui.add_client(final_email, total_bytes, expiry_ms, inbound_ids)
            else:
                logging.info(f"Client {final_email} already exists, skipping add.")

            await xui.assign_group(final_email, str(user_id))
            await invalidate_cache(user_id)

            # Update invoice status
            with SessionLocal() as db:
                invoice = db.query(Invoice).filter(Invoice.id == invoice_id).first()
                if invoice:
                    invoice.status = "COMPLETE"
                    invoice.client_name = final_email  # Update to the actual email used on panel
                    db.commit()

            await animate_loading_message(invoice_id, ["⏳", "⌛", "⏳", "✨✅✨"], "✅ <b>سرویس شما با موفقیت ایجاد شد!</b>\n\n🌟 لطفاً برای مشاهده لینک اتصال و جزئیات، به منوی <b>📦 مدیریت سرویس‌های من</b> مراجعه کنید. 🙏")

            referral_notifications = mark_referral_paid(user_id)
            sub_link = await get_sub_link(final_email)
            link_text = f"\n\n🔗 <b>لینک اتصال شما:</b>\n<code>{sub_link}</code>" if sub_link else "\n\n⚠️ لینک اتصال هنوز تنظیم نشده است. لطفاً به منوی سرویس‌های من مراجعه کنید."
            notifications = referral_notifications + [
                (user_id,
                 f"🎉 <b>سرویس شما با موفقیت ایجاد شد!</b>\n\n"
                 f"🔖 نام سرویس: <code>{final_email}</code>\n"
                 f"📦 پلن: {plan_name}\n"
                 f"⏳ مدت: {duration_days} روز\n"
                 f"📶 حجم: {traffic_gb} GB\n"
                 f"{link_text}\n\n"
                 f"🌟 از اعتماد شما سپاسگزاریم!", MSG_EFFECT_CONFETTI)
            ]
            admin_ids = [int(x.strip()) for x in os.getenv('ADMIN_CHAT_IDS', '').split(',') if x.strip()]
            for aid in admin_ids:
                notifications.append((aid, f"✅ <b>سرویس جدید ایجاد شد</b>\n\n👤 <b>شناسه کاربر:</b> <code>{user_id}</code>\n📦 <b>پلن:</b> {plan_name}\n🔗 <b>سرویس:</b> <code>{final_email}</code>\n📄 <b>شماره فاکتور:</b> #{invoice_id}"))

            await notify_many_users(notifications)
        except Exception as e:
            await animate_loading_message(invoice_id, ["⏳", "⌛", "⏳", "❌😔"], f"❌ <b>خطا در ساخت سروس:</b> {str(e)[:200]}\n\n🙏 لطفاً دوباره تلاش کنید یا با پشتیبانی تماس بگیرید.")
            raise
    run_async(_run())

@celery_app.task(bind=True, autoretry_for=(Exception,), retry_backoff=True, max_retries=5)
def provision_renew(self, invoice_id: int, email: str):
    async def _run():
        try:
            # Fetch invoice and plan, store needed data
            with SessionLocal() as db:
                invoice = db.query(Invoice).filter(Invoice.id == invoice_id).first()
                if not invoice:
                    return
                if invoice.status == "COMPLETE":
                    return
                plan = db.query(Plan).filter(Plan.id == invoice.plan_id).first()
                if not plan:
                    invoice.status = "FAILED"
                    db.commit()
                    await notify_user(invoice.telegram_user_id, "❌ پلن مربوطه یافت نشد.")
                    return
                user_id = invoice.telegram_user_id
                plan_name = plan.name
                traffic_gb = plan.traffic_gb
                duration_days = plan.duration_days

            xui = XUIClient()
            full_client_response = await xui.get_client_full(email)
            client_payload = full_client_response.get('client', {})
            
            now_ms = int(time.time() * 1000)
            used = full_client_response.get('usedTraffic', 0)
            remaining_bytes = max(0, client_payload.get('totalGB', 0) - used)
            remaining_days_ms = max(0, client_payload.get('expiryTime', 0) - now_ms)
            
            new_total_bytes = remaining_bytes + (traffic_gb * 1024 ** 3)
            new_expiry_ms = now_ms + remaining_days_ms + (duration_days * 86400 * 1000)

            update_payload = {
                "email": email,
                "totalGB": new_total_bytes,
                "expiryTime": new_expiry_ms,
                "tgId": client_payload.get('tgId', 0),
                "enable": True
            }
            
            await xui.update_client(email, update_payload)
            await xui.reset_client_traffic(email)
            await invalidate_cache(user_id)
            
            with SessionLocal() as db:
                invoice = db.query(Invoice).filter(Invoice.id == invoice_id).first()
                if invoice:
                    invoice.status = "COMPLETE"
                    db.commit()
            
            await animate_loading_message(invoice_id, ["⏳", "⌛", "⏳", "✨✅✨"], f"🔄 <b>تمدید سرویس <code>{email}</code> با موفقیت انجام شد!</b> 🌟")
            
            referral_notifications = mark_referral_paid(user_id)
            
            sub_link = await get_sub_link(email)
            link_text = f"\n\n🔗 <b>لینک اتصال شما:</b>\n<code>{sub_link}</code>" if sub_link else "\n\n⚠️ لینک اتصال هنوز تنظیم نشده است. لطفاً به منوی سرویس‌های من مراجعه کنید."
            notifications = referral_notifications + [
                (user_id,
                 f"🔄 <b>سرویس شما برای <code>{email}</code> با موفقیت تمدید شد!</b>\n\n"
                 f"📦 پلن: {plan_name}\n"
                 f"⏳ مدت: {duration_days} روز\n"
                 f"📶 حجم: {traffic_gb} GB\n"
                 f"{link_text}", MSG_EFFECT_FIRE)
            ]
            admin_ids = [int(x.strip()) for x in os.getenv('ADMIN_CHAT_IDS', '').split(',') if x.strip()]
            for aid in admin_ids:
                notifications.append((aid, f"✅ <b>تمدید انجام شد</b>\n📦 <b>پلن:</b> {plan_name}\n🔗 <b>سرویس:</b> <code>{email}</code>"))
            
            await notify_many_users(notifications)
        except Exception as e:
            await animate_loading_message(invoice_id, ["⏳", "⌛", "⏳", "❌😔"], f"❌ <b>خطا در تمدید:</b> {str(e)[:200]}\n\n🙏 لطفاً دوباره تلاش کنید یا با پشتیبانی تماس بگیرید.")
            raise
    run_async(_run())

@celery_app.task(bind=True, autoretry_for=(Exception,), retry_backoff=True, max_retries=5)
def provision_topup(self, invoice_id: int, email: str):
    async def _run():
        try:
            # Fetch invoice, store needed data
            with SessionLocal() as db:
                invoice = db.query(Invoice).filter(Invoice.id == invoice_id).first()
                if not invoice:
                    return
                if invoice.status == "COMPLETE":
                    return
                user_id = invoice.telegram_user_id
                added_gb = invoice.added_gb or 0

            xui = XUIClient()
            full_client_response = await xui.get_client_full(email)
            client_payload = full_client_response.get('client', {})
            
            added_bytes = added_gb * (1024 ** 3)
            new_total_bytes = client_payload.get('totalGB', 0) + added_bytes
            
            update_payload = {
                "email": email,
                "totalGB": new_total_bytes,
                "expiryTime": client_payload.get('expiryTime', 0),
                "tgId": client_payload.get('tgId', 0),
                "enable": True
            }
            
            await xui.update_client(email, update_payload)
            await invalidate_cache(user_id)
            
            with SessionLocal() as db:
                invoice = db.query(Invoice).filter(Invoice.id == invoice_id).first()
                if invoice:
                    invoice.status = "COMPLETE"
                    db.commit()
            
            await animate_loading_message(invoice_id, ["⏳", "⌛", "⏳", "✨✅✨"], f"➕ <b>{added_gb} گیگابایت با موفقیت به <code>{email}</code> اضافه شد!</b> 🎉")
            
            referral_notifications = mark_referral_paid(user_id)
            
            sub_link = await get_sub_link(email)
            link_text = f"\n\n🔗 <b>لینک اتصال شما:</b>\n<code>{sub_link}</code>" if sub_link else "\n\n⚠️ لینک اتصال هنوز تنظیم نشده است. لطفاً به منوی سرویس‌های من مراجعه کنید."
            notifications = referral_notifications + [
                (user_id, 
                 f"➕ <b>مقدار {added_gb} گیگابایت با موفقیت به <code>{email}</code> اضافه شد!</b>\n\n"
                 f"{link_text}")
            ]
            admin_ids = [int(x.strip()) for x in os.getenv('ADMIN_CHAT_IDS', '').split(',') if x.strip()]
            for aid in admin_ids:
                notifications.append((aid, f"✅ <b>خرید حجم انجام شد</b> ({added_gb}GB) برای <code>{email}</code>"))
            
            await notify_many_users(notifications)
        except Exception as e:
            await animate_loading_message(invoice_id, ["⏳", "⌛", "⏳", "❌😔"], f"❌ <b>خطا در افزودن حجم:</b> {str(e)[:200]}\n\n🙏 لطفاً دوباره تلاش کنید یا با پشتیبانی تماس بگیرید.")
            raise
    run_async(_run())

# ----- Regular user service deletion -----
@celery_app.task(bind=True, autoretry_for=(Exception,), retry_backoff=True, max_retries=3)
def delete_user_service(self, invoice_id: int):
    """Delete a user-created service: disable client on panel and mark invoice as DELETED.
    No refund is performed (regular users don't have an allowance pool)."""
    async def _run():
        # Fetch the invoice and user details
        with SessionLocal() as db:
            inv = db.query(Invoice).filter(Invoice.id == invoice_id).first()
            if not inv:
                return
            email = inv.client_name
            user_id = inv.telegram_user_id
            if not email or not user_id:
                return

        try:
            xui = XUIClient()
            # Get current client data
            full = await xui.get_client_full(email)
            if not full or 'client' not in full:
                # Client not found on panel — just mark as DELETED
                with SessionLocal() as db:
                    inv = db.query(Invoice).filter(Invoice.id == invoice_id).first()
                    if inv and inv.status not in ('DELETED', 'FAILED'):
                        inv.status = "DELETED"
                        db.commit()
                await notify_user(user_id, f"⚠️ <b>سرویس <code>{email}</code> حذف شد.</b>\n\n(سرویس در پنل یافت نشد، فقط وضعیت به‌روز شد.)")
                return

            client_data = full['client']
            # Disable the client
            update_payload = {
                "email": email,
                "totalGB": client_data.get('totalGB', 0),
                "expiryTime": client_data.get('expiryTime', 0),
                "tgId": client_data.get('tgId', 0),
                "enable": False
            }
            await xui.update_client(email, update_payload)

            # Mark invoice as DELETED
            with SessionLocal() as db:
                inv = db.query(Invoice).filter(Invoice.id == invoice_id).first()
                if inv and inv.status not in ('DELETED', 'FAILED'):
                    inv.status = "DELETED"
                    db.commit()

            await notify_user(user_id, f"🗑 <b>سرویس <code>{email}</code> با موفقیت حذف شد.</b>\n\nسرویس غیرفعال شد و دیگر قابل استفاده نیست.")

        except Exception as e:
            # On failure, if retries exhausted, mark as FAILED and notify
            if self.request.retries >= self.max_retries:
                logging.error(f"User service deletion failed after {self.max_retries} retries: {e}")
                with SessionLocal() as db:
                    inv = db.query(Invoice).filter(Invoice.id == invoice_id).first()
                    if inv and inv.status not in ('DELETED', 'FAILED'):
                        inv.status = "FAILED"
                        db.commit()
                await notify_user(user_id, f"❌ <b>حذف سرویس <code>{email}</code> ناموفق بود.</b>\n\nلطفاً دوباره تلاش کنید یا به مدیریت اطلاع دهید.")
                raise
            else:
                logging.warning(f"User service deletion attempt {self.request.retries+1} failed: {e}")
                raise

    run_async(_run())

# ----- Automated tasks -----
@celery_app.task
def trigger_daily_backup():
    async def _run():
        xui = XUIClient()
        panel_ok = False
        panel_error = None
        bot_ok = False
        bot_error = None

        try:
            await xui.backup_to_telegram()
            panel_ok = True
        except Exception as e:
            panel_error = str(e)

        try:
            backup_name, backup_content = build_bot_db_backup()
            await send_admin_document(
                backup_name,
                backup_content,
                "💾 <b>بکاپ روزانه دیتابیس ربات</b>"
            )
            bot_ok = True
        except Exception as e:
            bot_error = str(e)

        if panel_ok and bot_ok:
            await notify_admins("✅ <b>بکاپ روزانه کامل شد.</b>\n\n• بکاپ پنل 3x-ui ارسال شد\n• بکاپ دیتابیس ربات برای ادمین‌ها ارسال شد")
            return

        problems = []
        if not panel_ok:
            problems.append(f"• بکاپ پنل ناموفق بود: <code>{panel_error}</code>")
        if not bot_ok:
            problems.append(f"• بکاپ دیتابیس ربات ناموفق بود: <code>{bot_error}</code>")
        await notify_admins("❌ <b>بکاپ روزانه ناقص بود.</b>\n\n" + "\n".join(problems))

    run_async(_run())

@celery_app.task
def alert_low_traffic_expiry():
    async def _run():
        with SessionLocal() as db:
            xui = XUIClient()
            now_ms = int(time.time() * 1000)
            # Build user mapping from COMPLETE invoices: email -> user_id
            invoices = db.query(Invoice).filter(Invoice.status == "COMPLETE").all()
            user_mapping = {}
            email_set = set()
            for inv in invoices:
                if inv.client_name:
                    user_mapping[inv.client_name] = inv.telegram_user_id
                    email_set.add(inv.client_name)
            
            notifications = []
            for email in email_set:
                try:
                    full_response = await xui.get_client_full(email)
                    if not full_response or 'client' not in full_response:
                        continue
                    client_data = full_response['client']
                    if not client_data.get('enable'):
                        continue
                    total = client_data.get('totalGB', 0)
                    used = full_response.get('usedTraffic', 0)
                    exp = client_data.get('expiryTime', 0)
                    
                    is_low_vol = (total > 0) and ((total - used) < (2 * 1024**3))
                    is_expiring = (exp > 0) and (0 < (exp - now_ms) < (3 * 86400 * 1000))
                    tg_id = user_mapping.get(email)
                    if tg_id and (is_low_vol or is_expiring):
                        msg = (f"⚠️ <b>هشدار اتمام سرویس</b>\n━━━━━━━━━━━━━━\n"
                               f"کاربر گرامی، سرویس <code>{email}</code> رو به پایان است.\n\n"
                               f"جهت جلوگیری از قطعی، لطفاً از منوی ربات اقدام به <b>تمدید</b> یا <b>خرید حجم</b> نمایید.")
                        notifications.append((tg_id, msg))
                except Exception:
                    continue
            if notifications:
                await notify_many_users(notifications)
    run_async(_run())

# ----- Stalled invoice recovery (every 15 min) -----
@celery_app.task
def check_stalled_invoices():
    """Revert stuck work back to PENDING; expire stale PENDING invoices after 24h."""
    async def _run():
        with SessionLocal() as db:
            now_aware = datetime.now(timezone.utc)
            cutoff = now_aware - timedelta(minutes=30)
            stuck = db.query(Invoice).filter(
                Invoice.status.in_(["LOCKED", "PROCESSING"]),
                Invoice.created_at < cutoff
            ).all()
            for inv in stuck:
                inv.status = "PENDING"
            
            # Expire stale PENDING invoices older than 24 hours
            pending_cutoff = now_aware - timedelta(hours=24)
            stale = db.query(Invoice).filter(
                Invoice.status == "PENDING",
                Invoice.created_at < pending_cutoff
            ).all()
            for inv in stale:
                inv.status = "EXPIRED"
            
            if stuck or stale:
                db.commit()
                msg_parts = []
                if stuck:
                    msg_parts.append(f"🔄 {len(stuck)} فاکتور LOCKED/PROCESSING → PENDING")
                if stale:
                    msg_parts.append(f"🗑 {len(stale)} فاکتور PENDING → EXPIRED")
                await notify_admins(f"<b>بازیابی خودکار فاکتورها</b>\n" + "\n".join(msg_parts))
            else:
                db.commit()
    run_async(_run())

# ----- Panel traffic collection (daily) -----
@celery_app.task
def collect_panel_traffic():
    """Collect daily panel-wide traffic stats from /panel/api/server/status.
    Computes daily delta from the previous day's cumulative values.
    Prunes records older than 90 days.
    """
    from database import PanelTraffic  # import here to avoid circular import at module level
    async def _run():
        xui = XUIClient()
        try:
            status = await xui.get_server_status()
        except Exception as e:
            logging.error(f"Failed to fetch server status for traffic collection: {e}")
            return
        net_io = status.get('netIO', {})
        if not net_io:
            logging.warning("No netIO in server status response")
            return
        current_up = net_io.get('up', 0)
        current_down = net_io.get('down', 0)
        today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        
        with SessionLocal() as db:
            # Get the most recent record
            last = db.query(PanelTraffic).order_by(PanelTraffic.date.desc()).first()
            if last:
                # Compute deltas
                delta_up = current_up - last.cumulative_up
                delta_down = current_down - last.cumulative_down
                # If counters reset (delta negative), treat as 0 for that day
                if delta_up < 0:
                    delta_up = 0
                if delta_down < 0:
                    delta_down = 0
            else:
                # First run: no previous data, set delta to 0
                delta_up = 0
                delta_down = 0
            
            # Create new record
            new_record = PanelTraffic(
                date=today,
                cumulative_up=current_up,
                cumulative_down=current_down,
                daily_up=delta_up,
                daily_down=delta_down
            )
            db.add(new_record)
            db.commit()
            
            # Prune records older than 90 days
            cutoff = today - timedelta(days=90)
            old_records = db.query(PanelTraffic).filter(PanelTraffic.date < cutoff).all()
            if old_records:
                for rec in old_records:
                    db.delete(rec)
                db.commit()
                logging.info(f"Pruned {len(old_records)} panel traffic records older than 90 days")
    run_async(_run())

@celery_app.task
def check_expired_services_for_deletion():
    """Check for expired services and handle deletion warnings.
    
    Logic:
    1. Find all COMPLETE invoices where service has expired
    2. If expiry + 10 days has passed, set deletion_scheduled_at to expiry + 10 (if not already set)
    3. Send warning messages on days 7, 8, 9 after expiry (3 consecutive warnings)
    4. If deletion_scheduled_at has passed AND 3 warnings sent, delete the service
    """
    async def _run():
        now = datetime.now(timezone.utc)
        with SessionLocal() as db:
            # Find all complete invoices with client emails
            invoices = db.query(Invoice).filter(
                Invoice.status == "COMPLETE",
                Invoice.client_name.isnot(None)
            ).all()
            
            xui = XUIClient()
            notifications = []
            services_to_delete = []
            
            for inv in invoices:
                try:
                    full_response = await xui.get_client_full(inv.client_name)
                    if not full_response or 'client' not in full_response:
                        continue
                    
                    client_data = full_response['client']
                    expiry_time = client_data.get('expiryTime', 0)
                    
                    if expiry_time <= 0:
                        continue  # No expiry set
                    
                    expiry_date = datetime.fromtimestamp(expiry_time / 1000, tz=timezone.utc)
                    days_since_expiry = (now - expiry_date).days
                    
                    # Check if service is expired
                    if days_since_expiry < 0:
                        continue  # Not expired yet
                    
                    # If 7+ days since expiry, schedule for deletion at day 10
                    if days_since_expiry >= 7:
                        # Set deletion_scheduled_at if not already set (schedule for expiry + 10 days)
                        if inv.deletion_scheduled_at is None:
                            inv.deletion_scheduled_at = expiry_date + timedelta(days=10)
                            db.commit()
                        
                        # Calculate days until deletion
                        days_until_deletion = (inv.deletion_scheduled_at - now).days
                        
                        # Send warnings on days 7, 8, 9 after expiry (before deletion on day 10)
                        # Only send if we haven't sent 3 warnings yet
                        if inv.deletion_warning_sent_count < 3:
                            if days_since_expiry in [7, 8, 9]:
                                # Get service name/identifier
                                service_name = inv.client_name
                                
                                warning_text = (
                                    f"⚠️ <b>هشدار حذف سرویس</b>\n"
                                    f"━━━━━━━━━━━━━━━━━━\n"
                                    f"📧 سرویس: <code>{service_name}</code>\n"
                                    f"📅 تاریخ انقضا: {expiry_date.strftime('%Y-%m-%d')}\n"
                                    f"⏰ مهلت باقیمانده: {days_until_deletion} روز\n\n"
                                    f"❌ در صورت عدم تمدید، این سرویس پس از اتمام مهلت <b>حذف خواهد شد</b>.\n\n"
                                    f"💡 برای تمدید از منوی «سرویس‌های من» اقدام کنید."
                                )
                                notifications.append((inv.telegram_user_id, warning_text))
                                inv.deletion_warning_sent_count += 1
                                db.commit()
                        
                        # Check if it's time to delete (deletion date passed AND 3 warnings sent)
                        if days_until_deletion <= 0 and inv.deletion_warning_sent_count >= 3:
                            services_to_delete.append({
                                'invoice': inv,
                                'email': inv.client_name,
                                'user_id': inv.telegram_user_id
                            })
                    
                except Exception as e:
                    logging.error(f"Error checking invoice {inv.id}: {e}")
                    continue
            
            # Delete services that have passed the deletion deadline
            for item in services_to_delete:
                try:
                    email = item['email']
                    user_id = item['user_id']
                    inv = item['invoice']
                    
                    # Delete from XUI panel
                    await xui.delete_client(email)
                    
                    # Mark invoice as deleted
                    inv.status = "DELETED"
                    db.commit()
                    
                    # Notify user
                    notify_text = (
                        f"❌ <b>سرویس شما حذف شد</b>\n"
                        f"━━━━━━━━━━━━━━━━━━\n"
                        f"📧 سرویس: <code>{email}</code>\n\n"
                        f"متأسفانه به دلیل عدم تمدید در مهلت مقرر، این سرویس حذف شده است.\n\n"
                        f"💡 برای خرید سرویس جدید از منوی اصلی اقدام کنید."
                    )
                    notifications.append((user_id, notify_text))
                    
                except Exception as e:
                    logging.error(f"Error deleting service {email}: {e}")
                    # Refund reseller if applicable
                    if inv.reseller_id:
                        _refund_reseller_services(email, inv.reseller_id)
            
            # Send all notifications
            if notifications:
                await notify_many_users(notifications)
                
    run_async(_run())


# ----- Reseller pack expiry check (daily) -----
@celery_app.task
def check_reseller_pack_expiry():
    """Expire reseller packs that have passed their expiry date and send notifications."""
    async def _run():
        now = datetime.now(timezone.utc)
        with SessionLocal() as db:
            # Expire packs whose expiry date is in the past and are still active
            expired_packs = db.query(ResellerPack).filter(
                ResellerPack.is_active == True,
                ResellerPack.expiry_date < now
            ).all()
            for pack in expired_packs:
                pack.is_active = False
                unused = max(0, pack.granted_bytes - pack.used_bytes)
                if unused > 0:
                    reseller_id = pack.reseller_id
                    text = (f"⏰ <b>بسته ترافیک نمایندگی منقضی شد</b>\n"
                            f"━━━━━━━━━━━━━━━━━━\n"
                            f"📦 حجم بسته: {format_size(pack.granted_bytes)}\n"
                            f"📥 مصرف‌شده: {format_size(pack.used_bytes)}\n"
                            f"📤 باقیمانده (منقضی): {format_size(unused)}\n"
                            f"📅 تاریخ انقضا: {pack.expiry_date.strftime('%Y-%m-%d')}\n\n"
                            f"⚠️ حجم باقیمانده از دسترس خارج شد.")
                    await notify_user(reseller_id, text)
            
            # Warn about packs expiring in the next 5 days
            warning_cutoff = now + timedelta(days=5)
            warning_packs = db.query(ResellerPack).filter(
                ResellerPack.is_active == True,
                ResellerPack.expiry_date >= now,
                ResellerPack.expiry_date <= warning_cutoff
            ).all()
            for pack in warning_packs:
                reseller_id = pack.reseller_id
                unused = max(0, pack.granted_bytes - pack.used_bytes)
                days_left = (pack.expiry_date - now).days
                if days_left > 0 and unused > 0:
                    text = (f"⚠️ <b>هشدار انقضای بسته ترافیک</b>\n"
                            f"━━━━━━━━━━━━━━━━━━\n"
                            f"📦 حجم بسته: {format_size(pack.granted_bytes)}\n"
                            f"📥 مصرف‌شده: {format_size(pack.used_bytes)}\n"
                            f"📤 باقیمانده: {format_size(unused)}\n"
                            f"📅 انقضا در {days_left} روز (تا {pack.expiry_date.strftime('%Y-%m-%d')})\n\n"
                            f"💡 لطفاً قبل از انقضا از ترافیک باقیمانده استفاده کنید.")
                    await notify_user(reseller_id, text)

            db.commit()
    run_async(_run())


async def notify_user_with_buttons(tg_id: int, text: str, inline_keyboard: list):
    """Send a DM with an inline keyboard. ``inline_keyboard`` is Telegram's raw
    layout — a list of button-rows, each a list of {"text","callback_data"} dicts.
    Workers have no aiogram Bot, so this talks to the Telegram HTTP API directly
    (mirrors ``notify_user``)."""
    bot_token = os.getenv('BOT_TOKEN')
    if not bot_token:
        return
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {"chat_id": tg_id, "text": text, "parse_mode": "HTML"}
    if inline_keyboard:
        payload["reply_markup"] = {"inline_keyboard": inline_keyboard}
    async with httpx.AsyncClient() as client:
        await client.post(url, json=payload)


# ----- Reconcile invisible services: repair client_name to the real panel email -----
@celery_app.task
def reconcile_client_names(admin_id: int, user_filter: str = None):
    """Full-panel dry-run scan for invoices whose ``client_name`` doesn't match the
    real panel email — the mismatch is what makes a service invisible in
    "My Services". Scope is name repair only (no orphan deletion).

    Runs on the worker because a full scan touches every panel group and probes
    the panel for missing NEW clients, so it takes several minutes (worker
    concurrency=1). It writes NOTHING: it stashes the proposed plan in Redis under
    ``reconcile_plan:{admin_id}`` (TTL 1h) and DMs the admin a summary with a
    tap-to-apply button. Applying is fast (DB writes + cache clear) and is done
    inline by the bot's ``reconcile_apply`` handler off the stashed plan, so
    tapping the button never triggers a second multi-minute scan.
    """
    async def _run():
        xui = XUIClient()
        try:
            result = await compute_reconcile(xui, user_filter)
        finally:
            await xui.close()

        fixes = result["fixes"]
        plan = to_plan(fixes)
        total = len(result["records"])

        # Stash the plan so the apply button doesn't need to re-scan the panel.
        r = redis.Redis.from_url(REDIS_URL, decode_responses=True)
        try:
            if plan:
                await r.set(f"reconcile_plan:{admin_id}", json.dumps(plan), ex=3600)
            else:
                await r.delete(f"reconcile_plan:{admin_id}")
        finally:
            await r.aclose()

        scope = f" (کاربر <code>{user_filter}</code>)" if user_filter else ""
        header = (
            f"🔧 <b>بازسازی نام سرویس‌ها</b>{scope}\n"
            "━━━━━━━━━━━━━━━━━━\n"
            f"🔎 بررسی‌شده: <b>{total}</b> فاکتور در <b>{result['groups']}</b> گروه\n"
            f"🛠 قابل تعمیر: <b>{len(fixes)}</b>\n"
            f"✅ از قبل درست: <b>{len(result['skipped'])}</b>\n"
            f"❓ مبهم (بررسی دستی): <b>{len(result['ambiguous'])}</b>\n"
            f"🚫 روی پنل نیست: <b>{len(result['not_found'])}</b>"
        )
        if plan:
            text = header + f"\n\n👇 برای اعمال <b>{len(plan)}</b> تعمیر، دکمه زیر را بزنید."
            kb = [[{"text": f"✅ اعمال {len(plan)} تعمیر", "callback_data": "reconcile_apply"}]]
            await notify_user_with_buttons(admin_id, text, kb)
        else:
            await notify_user(admin_id, header + "\n\n✅ موردی برای تعمیر یافت نشد.")
    run_async(_run())

@celery_app.task(bind=True, autoretry_for=(Exception,), retry_backoff=True, max_retries=5)
def provision_reseller_extend(self, invoice_id: int):
    """Extend an owned reseller service without changing or resetting traffic."""
    async def _run():
        with SessionLocal() as db:
            invoice = db.query(Invoice).filter(Invoice.id == invoice_id).first()
            if not invoice:
                return
            reseller_id = invoice.reseller_id
            email = invoice.client_name
            try:
                days = int(json.loads(invoice.description or '{}').get('days', 0))
            except (TypeError, ValueError, json.JSONDecodeError):
                days = 0
            owns_service = db.query(Invoice.id).filter(
                Invoice.reseller_id == reseller_id,
                Invoice.client_name == email,
                Invoice.status == 'COMPLETE'
            ).first() is not None
        if not reseller_id or not email or days <= 0 or not owns_service:
            _refund_reseller(invoice_id)
            return
        try:
            xui = XUIClient()
            full_client_response = await xui.get_client_full(email)
            client_payload = full_client_response.get('client', {})
            if not client_payload:
                raise ValueError(f"Client not found: {email}")
            now_ms = int(time.time() * 1000)
            current_expiry = client_payload.get('expiryTime', 0)
            expiry_base = max(now_ms, current_expiry) if current_expiry > 0 else now_ms
            update_payload = {
                "email": email,
                "totalGB": client_payload.get('totalGB', 0),
                "expiryTime": expiry_base + (days * 86400 * 1000),
                "tgId": client_payload.get('tgId', 0),
                "enable": client_payload.get('enable', True)
            }
            await xui.update_client(email, update_payload)
            await invalidate_cache(reseller_id)
            with SessionLocal() as db:
                inv = db.query(Invoice).filter(Invoice.id == invoice_id).first()
                inv.status = "COMPLETE"
                db.commit()
            await notify_user(reseller_id, f"⏳ <b>{days} روز به سرویس <code>{email}</code> اضافه شد.</b>")
        except Exception as e:
            if self.request.retries >= self.max_retries:
                _refund_reseller(invoice_id)
                await notify_user(reseller_id, "❌ <b>افزایش مدت سرویس ناموفق بود.</b>")
                logging.error(f"Reseller extend failed after {self.max_retries} retries: {e}")
            else:
                logging.warning(f"Reseller extend attempt {self.request.retries+1} failed: {e}")
            raise
    run_async(_run())


# ===================== Amnezia service tasks =====================
# Quota/expiry live on the PANEL USER (shared by all of a user's Amnezia
# connections); traffic_limit is sent in whole GB, read back in bytes.
# The chosen server for AMNEZIA_NEW rides in invoice.description JSON
# ({"server_id": N}) — same pattern provision_reseller_extend uses for days.

async def send_user_document(tg_id: int, filename: str, content: bytes, caption: str):
    bot_token = os.getenv('BOT_TOKEN')
    if not bot_token:
        return
    url = f"https://api.telegram.org/bot{bot_token}/sendDocument"
    async with httpx.AsyncClient(timeout=60.0) as client:
        files = {"document": (filename, content, "application/octet-stream")}
        data = {"chat_id": str(tg_id), "caption": caption, "parse_mode": "HTML"}
        try:
            await client.post(url, data=data, files=files)
        except Exception:
            pass


def _invoice_server_id(description: str):
    """Extract {"server_id": N} from an invoice description JSON (None if absent)."""
    try:
        return int(json.loads(description or '{}').get('server_id'))
    except (TypeError, ValueError, json.JSONDecodeError):
        return None


def _invoice_amnezia_creds(description: str) -> tuple:
    """(username, password) the user chose for their Amnezia panel account,
    stored in the invoice description JSON. Either may be None."""
    try:
        d = json.loads(description or '{}')
    except (TypeError, json.JSONDecodeError):
        return None, None
    username = d.get('amz_username') if isinstance(d.get('amz_username'), str) else None
    password = d.get('amz_password') if isinstance(d.get('amz_password'), str) else None
    return username, password


def _get_amnezia_mapping(db, telegram_id: int):
    """Legacy 1:1 mapping lookup — kept only for reference/migration tooling."""
    from database import AmneziaUser
    return db.query(AmneziaUser).filter(AmneziaUser.telegram_user_id == telegram_id).first()


@celery_app.task(bind=True, autoretry_for=(Exception,), retry_backoff=True, max_retries=5)
def provision_amnezia_new(self, invoice_id: int):
    async def _run():
        try:
            with SessionLocal() as db:
                invoice = db.query(Invoice).filter(Invoice.id == invoice_id).first()
                if not invoice or invoice.status == "COMPLETE":
                    return
                plan = db.query(Plan).filter(Plan.id == invoice.plan_id).first()
                if not plan:
                    invoice.status = "FAILED"
                    db.commit()
                    await notify_user(invoice.telegram_user_id, "❌ پلن مربوطه یافت نشد. لطفاً با پشتیبانی تماس بگیرید.")
                    return
                user_id = invoice.telegram_user_id
                wanted_server = _invoice_server_id(invoice.description)
                amz_username, amz_password = _invoice_amnezia_creds(invoice.description)
                # Idempotency: a retry after a partial failure must not create a
                # second account or connection — reuse the service row from the
                # first attempt.
                existing = db.query(AmneziaService).filter(
                    AmneziaService.invoice_id == invoice_id,
                    AmneziaService.status == 'active').first()
                existing_conn = existing.connection_id if existing else None
                existing_client = existing.client_id if existing else None
                existing_server = existing.server_id if existing else None
                existing_name = existing.name if existing else None
                existing_acct = ({"panel_user_id": existing.panel_user_id,
                                  "username": existing.panel_username,
                                  "password": existing.panel_password}
                                 if existing and existing.panel_user_id else None)

            amz = AmneziaClient()
            try:
                acct = existing_acct or await amz.ensure_service_account(
                    user_id, base_username=amz_username, password=amz_password,
                    invoice_id=invoice_id)
                if existing_conn:
                    created = {"connection_id": existing_conn, "client_id": existing_client,
                               "server_id": existing_server, "protocol": amz.protocol,
                               "name": existing_name, "server_name": None,
                               "config": None, "vpn_link": None}
                else:
                    servers = await amz.list_servers()
                    if not servers:
                        raise RuntimeError("no Amnezia servers discovered")
                    valid_ids = {s['id'] for s in servers}
                    server_id = wanted_server if wanted_server in valid_ids else servers[0]['id']
                    conn_name = f"amz_{user_id}_{invoice_id}"
                    created = await amz.add_connection(acct["panel_user_id"], server_id, conn_name)
                    created["name"] = conn_name

                now = datetime.now(timezone.utc)
                expiry = now + timedelta(days=plan.duration_days)
                await amz.update_user_limits(acct["panel_user_id"], user_id,
                                             quota_gb=plan.traffic_gb, expiry=expiry)

                with SessionLocal() as db:
                    svc = db.query(AmneziaService).filter(
                        AmneziaService.connection_id == created['connection_id']).first()
                    if svc is None:
                        svc = AmneziaService(
                            telegram_user_id=user_id,
                            connection_id=created['connection_id'],
                            client_id=created['client_id'],
                            server_id=created['server_id'],
                            server_name=created.get('server_name'),
                            protocol=created.get('protocol') or amz.protocol,
                            name=created.get('name') or f"amz_{user_id}_{invoice_id}",
                            quota_bytes=plan.traffic_gb * 1024 ** 3,
                            expiry_date=expiry,
                            status='active',
                            invoice_id=invoice_id,
                            panel_user_id=acct["panel_user_id"],
                            panel_username=acct["username"],
                            panel_password=acct["password"],
                        )
                        db.add(svc)
                        db.flush()
                    else:
                        svc.status = 'active'
                        svc.quota_bytes = plan.traffic_gb * 1024 ** 3
                        svc.expiry_date = expiry
                        svc.panel_user_id = acct["panel_user_id"]
                        svc.panel_username = acct["username"]
                        svc.panel_password = acct["password"]
                        if created.get('server_name'):
                            svc.server_name = created['server_name']
                    invoice = db.query(Invoice).filter(Invoice.id == invoice_id).first()
                    if invoice:
                        invoice.status = "COMPLETE"
                        invoice.amnezia_service_id = svc.id
                        invoice.client_name = svc.name
                    db.commit()
                    service_id = svc.id

                await animate_loading_message(invoice_id, ["⏳", "⌛", "⏳", "✨✅✨"],
                                              "✅ <b>سرویس Amnezia شما با موفقیت ایجاد شد!</b>")

                referral_notifications = mark_referral_paid(user_id)

                # Deliver the fresh config from the panel (a retry has none stored).
                cfg = await amz.get_connection_config(created['server_id'], created['client_id'])
                vpn_link = cfg.get('vpn_link') or created.get('vpn_link')
                config_text = cfg.get('config') or ''
                if vpn_link:
                    await notify_user(user_id,
                        f"🔗 <b>لینک اتصال Amnezia:</b>\n<code>{vpn_link}</code>\n\n"
                        f"👆 این لینک را کپی و در برنامه Amnezia وارد کنید.")
                if config_text:
                    await send_user_document(user_id, f"{created.get('name') or 'amnezia'}.conf",
                                             config_text.encode('utf-8'),
                                             "📄 فایل کانفیگ سرویس Amnezia شما")

                # Always show THIS subscription's panel-account credentials back
                # to the user so they can log into the Amnezia web UI.
                cred_lines = (
                    f"👤 <b>نام کاربری:</b> <code>{acct['username']}</code>\n"
                    + (f"🔑 <b>رمز عبور:</b> <code>{acct['password']}</code>"
                       if acct.get('password') else
                       "🔑 رمز عبور: <i>در دسترس نیست.</i>")
                    + f"\n\n🌐 ورود به پنل وب: {os.getenv('AMNEZIA_API_URL', '').rstrip('/')}"
                )
                await notify_user(user_id, f"🔐 <b>مشخصات حساب Amnezia این سرویس</b>\n\n{cred_lines}")

                # Point the buyer at the official AmneziaVPN apps (URL buttons).
                await notify_user_with_buttons(user_id,
                    "📱 <b>برنامه Amnezia را دانلود کنید:</b>",
                    [[{"text": "▶️ Google Play", "url": PLAY_STORE_URL},
                      {"text": "🍎 App Store", "url": APP_STORE_URL}]])

                # Self-service FAQ — answers the questions support otherwise would.
                await notify_user_with_buttons(user_id,
                    "❓ <b>تازه شروع کردید؟</b> راهنمای قدم‌به‌قدم و رفع اشکال:",
                    [[{"text": "❓ راهنما و رفع اشکال Amnezia", "callback_data": "amzfaq"}]])

                notifications = referral_notifications + [
                    (user_id,
                     f"🎉 <b>سرویس Amnezia شما فعال شد!</b>\n\n"
                     f"🔖 نام سرویس: <code>{created.get('name')}</code>\n"
                     f"📦 حجم: {plan.traffic_gb} GB\n"
                     f"⏳ مدت: {plan.duration_days} روز\n\n"
                     f"🌟 برای مشاهده لینک و کانفیگ، به منوی «📦 مدیریت سرویس‌های من» مراجعه کنید.",
                     MSG_EFFECT_CONFETTI)
                ]
                admin_ids = [int(x.strip()) for x in os.getenv('ADMIN_CHAT_IDS', '').split(',') if x.strip()]
                for aid in admin_ids:
                    notifications.append((aid,
                        f"✅ <b>سرویس Amnezia جدید ایجاد شد</b>\n\n"
                        f"👤 <b>شناسه کاربر:</b> <code>{user_id}</code>\n"
                        f"📦 <b>پلن:</b> {plan.name}\n"
                        f"🖥 <b>سرور:</b> {created.get('server_name') or created['server_id']}\n"
                        f"🔗 <b>سرویس:</b> <code>{created.get('name')}</code>\n"
                        f"📄 <b>شماره فاکتور:</b> #{invoice_id}"))
                await notify_many_users(notifications)
            finally:
                await amz.close()
        except Exception as e:
            await animate_loading_message(invoice_id, ["⏳", "⌛", "⏳", "❌😔"],
                                          f"❌ <b>خطا در ساخت سرویس Amnezia:</b> {str(e)[:200]}\n\n🙏 لطفاً دوباره تلاش کنید یا با پشتیبانی تماس بگیرید.")
            raise
    run_async(_run())


@celery_app.task(bind=True, autoretry_for=(Exception,), retry_backoff=True, max_retries=5)
def provision_amnezia_renew(self, invoice_id: int):
    """Renew: unused traffic carries over, plan GB is added, expiry extends."""
    async def _run():
        try:
            with SessionLocal() as db:
                invoice = db.query(Invoice).filter(Invoice.id == invoice_id).first()
                if not invoice or invoice.status == "COMPLETE":
                    return
                plan = db.query(Plan).filter(Plan.id == invoice.plan_id).first()
                svc = db.query(AmneziaService).filter(
                    AmneziaService.id == invoice.amnezia_service_id).first()
                if not plan or not svc:
                    invoice.status = "FAILED"
                    db.commit()
                    await notify_user(invoice.telegram_user_id, "❌ سرویس یا پلن مربوطه یافت نشد.")
                    return
                user_id = invoice.telegram_user_id
                service_id = svc.id
                panel_user_id, panel_username = svc.panel_user_id, svc.panel_username

            amz = AmneziaClient()
            try:
                if not panel_user_id or not panel_username:
                    raise RuntimeError(f"service {service_id} has no dedicated panel account")
                stats = await amz.get_user_stats(panel_username)
                if stats is None:
                    raise RuntimeError(f"panel user {panel_username} not found")
                now = datetime.now(timezone.utc)
                remaining_gb = max(0, round((stats['limit'] - stats['used']) / (1024 ** 3)))
                new_limit_gb = remaining_gb + plan.traffic_gb
                current_exp = stats['expiration_date']
                base = max(now, current_exp) if current_exp else now
                new_exp = base + timedelta(days=plan.duration_days)
                await amz.update_user_limits(panel_user_id, user_id,
                                             quota_gb=new_limit_gb, expiry=new_exp)
                with SessionLocal() as db:
                    svc = db.query(AmneziaService).filter(AmneziaService.id == service_id).first()
                    if svc:
                        svc.quota_bytes = new_limit_gb * 1024 ** 3
                        svc.expiry_date = new_exp
                        svc.status = 'active'
                        svc.expiry_warned_at = None
                    invoice = db.query(Invoice).filter(Invoice.id == invoice_id).first()
                    if invoice:
                        invoice.status = "COMPLETE"
                    db.commit()
                await animate_loading_message(invoice_id, ["⏳", "⌛", "⏳", "✨✅✨"],
                                              f"🔄 <b>تمدید سرویس Amnezia با موفقیت انجام شد!</b> 🌟")
                referral_notifications = mark_referral_paid(user_id)
                notifications = referral_notifications + [
                    (user_id,
                     f"🔄 <b>سرویس Amnezia شما تمدید شد!</b>\n\n"
                     f"📦 حجم افزوده‌شده: {plan.traffic_gb} GB\n"
                     f"⏳ مدت افزوده‌شده: {plan.duration_days} روز\n"
                     f"📅 انقضای جدید: {new_exp.strftime('%Y-%m-%d')}",
                     MSG_EFFECT_FIRE)
                ]
                admin_ids = [int(x.strip()) for x in os.getenv('ADMIN_CHAT_IDS', '').split(',') if x.strip()]
                for aid in admin_ids:
                    notifications.append((aid,
                        f"✅ <b>تمدید Amnezia انجام شد</b>\n👤 <code>{user_id}</code> | 📦 {plan.name} | 📄 #{invoice_id}"))
                await notify_many_users(notifications)
            finally:
                await amz.close()
        except Exception as e:
            await animate_loading_message(invoice_id, ["⏳", "⌛", "⏳", "❌😔"],
                                          f"❌ <b>خطا در تمدید سرویس Amnezia:</b> {str(e)[:200]}\n\n🙏 لطفاً دوباره تلاش کنید یا با پشتیبانی تماس بگیرید.")
            raise
    run_async(_run())


@celery_app.task(bind=True, autoretry_for=(Exception,), retry_backoff=True, max_retries=5)
def provision_amnezia_topup(self, invoice_id: int):
    """Top-up: adds volume only; expiry and remaining traffic are untouched."""
    async def _run():
        try:
            with SessionLocal() as db:
                invoice = db.query(Invoice).filter(Invoice.id == invoice_id).first()
                if not invoice or invoice.status == "COMPLETE":
                    return
                svc = db.query(AmneziaService).filter(
                    AmneziaService.id == invoice.amnezia_service_id).first()
                if not svc:
                    invoice.status = "FAILED"
                    db.commit()
                    await notify_user(invoice.telegram_user_id, "❌ سرویس مربوطه یافت نشد.")
                    return
                if (svc.quota_bytes or 0) == 0:
                    # Unlimited service: topping up would CAP the account-wide
                    # panel quota to the purchased GB. Refuse and refund nothing
                    # (invoice stays PENDING for admin review).
                    invoice.status = "NEEDS_REVIEW"
                    db.commit()
                    await notify_user(invoice.telegram_user_id,
                        "♾️ این سرویس نامحدود است؛ خرید حجم اضافه روی آن امکان‌پذیر نیست. "
                        "لطفاً با پشتیبانی تماس بگیرید.")
                    return
                user_id = invoice.telegram_user_id
                added_gb = invoice.added_gb or 0
                service_id = svc.id
                panel_user_id, panel_username = svc.panel_user_id, svc.panel_username

            amz = AmneziaClient()
            try:
                if not panel_user_id or not panel_username:
                    raise RuntimeError(f"service {service_id} has no dedicated panel account")
                stats = await amz.get_user_stats(panel_username)
                if stats is None:
                    raise RuntimeError(f"panel user {panel_username} not found")
                current_limit_gb = round(stats['limit'] / (1024 ** 3))
                new_limit_gb = current_limit_gb + added_gb
                await amz.update_user_limits(panel_user_id, user_id,
                                             quota_gb=new_limit_gb)
                with SessionLocal() as db:
                    svc = db.query(AmneziaService).filter(AmneziaService.id == service_id).first()
                    if svc:
                        svc.quota_bytes = new_limit_gb * 1024 ** 3
                        svc.status = 'active'
                    invoice = db.query(Invoice).filter(Invoice.id == invoice_id).first()
                    if invoice:
                        invoice.status = "COMPLETE"
                    db.commit()
                await animate_loading_message(invoice_id, ["⏳", "⌛", "⏳", "✨✅✨"],
                                              f"➕ <b>{added_gb} گیگابایت با موفقیت به سرویس Amnezia شما اضافه شد!</b> 🎉")
                referral_notifications = mark_referral_paid(user_id)
                notifications = referral_notifications + [
                    (user_id, f"➕ <b>{added_gb} گیگابایت به سرویس Amnezia شما اضافه شد!</b>")
                ]
                admin_ids = [int(x.strip()) for x in os.getenv('ADMIN_CHAT_IDS', '').split(',') if x.strip()]
                for aid in admin_ids:
                    notifications.append((aid,
                        f"✅ <b>خرید حجم Amnezia انجام شد</b> ({added_gb}GB) برای <code>{user_id}</code> | 📄 #{invoice_id}"))
                await notify_many_users(notifications)
            finally:
                await amz.close()
        except Exception as e:
            await animate_loading_message(invoice_id, ["⏳", "⌛", "⏳", "❌😔"],
                                          f"❌ <b>خطا در افزودن حجم Amnezia:</b> {str(e)[:200]}\n\n🙏 لطفاً دوباره تلاش کنید یا با پشتیبانی تماس بگیرید.")
            raise
    run_async(_run())


@celery_app.task(bind=True, autoretry_for=(Exception,), retry_backoff=True, max_retries=3)
def delete_amnezia_service(self, service_id: int):
    """Remove an Amnezia connection from the panel and mark the local row deleted."""
    async def _run():
        with SessionLocal() as db:
            svc = db.query(AmneziaService).filter(AmneziaService.id == service_id).first()
            if not svc or svc.status == 'deleted':
                return
            user_id = svc.telegram_user_id
            server_id = svc.server_id
            client_id = svc.client_id
            name = svc.name
            panel_user_id = svc.panel_user_id
        try:
            amz = AmneziaClient()
            try:
                if panel_user_id:
                    # Dedicated per-service account: deleting it cascades the
                    # connection too (panel-verified behaviour).
                    await amz.delete_panel_user(panel_user_id)
                else:
                    await amz.remove_connection(server_id, client_id)
            finally:
                await amz.close()
            with SessionLocal() as db:
                svc = db.query(AmneziaService).filter(AmneziaService.id == service_id).first()
                if svc:
                    svc.status = 'deleted'
                    db.commit()
            await notify_user(user_id,
                f"🗑 <b>سرویس Amnezia <code>{name}</code> حذف شد.</b>\n\nسرویس غیرفعال شد و دیگر قابل استفاده نیست.")
        except Exception as e:
            logging.error(f"Amnezia service deletion failed (service {service_id}): {e}")
            raise
    run_async(_run())


@celery_app.task(bind=True, autoretry_for=(Exception,), retry_backoff=True, max_retries=3)
def provision_amnezia_trial(self, user_id: int):
    """One free Amnezia trial per Telegram user.

    Entitlement lives in ``amnezia_trials`` (independent of XUI TrialUsage).
    Size/duration reuse the existing 🎁 trial settings. The service row is
    flagged ``is_trial=True`` so the expiry sweep DELETES the whole panel
    account at expiry — trials never accumulate on the panel.
    """
    import math

    async def _run():
        try:
            with SessionLocal() as db:
                # re-check entitlement (the handler guards too; retries may land later)
                if db.query(AmneziaTrial).filter(
                        AmneziaTrial.telegram_user_id == user_id).first():
                    return
                gb_s = db.query(AppSetting).filter(AppSetting.key == 'trial_traffic_gb').first()
                d_s = db.query(AppSetting).filter(AppSetting.key == 'trial_duration_days').first()
            traffic_gb = float(gb_s.value) if gb_s else 0.1
            days = int(d_s.value) if d_s else 1
            # Panel traffic_limit is whole-GB only — and 0 means UNLIMITED,
            # so a sub-GB trial must round UP, never down/truncate.
            trial_gb = max(1, math.ceil(traffic_gb))

            amz = AmneziaClient()
            try:
                acct = await amz.ensure_service_account(
                    user_id, base_username=f"trial{user_id}", invoice_id=None)
                servers = await amz.list_servers()
                alive = [s for s in servers if s.get('alive', True)]
                server_id = (alive or servers)[0]['id']
                conn_name = f"trial_{user_id}_{int(time.time())}"
                created = await amz.add_connection(acct['panel_user_id'], server_id, conn_name)

                now = datetime.now(timezone.utc)
                expiry = now + timedelta(days=days)
                await amz.update_user_limits(acct['panel_user_id'], user_id,
                                             quota_gb=trial_gb, expiry=expiry)

                with SessionLocal() as db:
                    svc = AmneziaService(
                        telegram_user_id=user_id,
                        connection_id=created['connection_id'],
                        client_id=created['client_id'],
                        server_id=created['server_id'],
                        server_name=created.get('server_name'),
                        protocol=created.get('protocol') or amz.protocol,
                        name=conn_name,
                        quota_bytes=trial_gb * 1024 ** 3,
                        expiry_date=expiry,
                        status='active',
                        is_trial=True,
                        panel_user_id=acct['panel_user_id'],
                        panel_username=acct['username'],
                        panel_password=acct['password'],
                    )
                    db.add(svc)
                    db.flush()
                    claim = AmneziaTrial(telegram_user_id=user_id, service_id=svc.id)
                    db.add(claim)
                    invoice = Invoice(
                        telegram_user_id=user_id,
                        total_price=0, original_price=0, discount_amount=0,
                        client_name=conn_name,
                        action_type='AMNEZIA_TRIAL', status='COMPLETE',
                        amnezia_service_id=svc.id,
                    )
                    db.add(invoice)
                    db.commit()
                    service_id = svc.id

                cfg = await amz.get_connection_config(created['server_id'], created['client_id'])
                vpn_link = cfg.get('vpn_link')
                config_text = cfg.get('config') or ''
                size_display = f"{traffic_gb:g}".rstrip('.') if traffic_gb < 1 else str(trial_gb)
                await notify_user(user_id,
                    f"🎁 <b>تست رایگان Amnezia شما فعال شد!</b>\n\n"
                    f"📦 حجم: {size_display} GB\n⏳ مدت: {days} روز\n"
                    f"⚠️ پس از انقضا، این سرویس به‌صورت خودکار حذف می‌شود.")
                if vpn_link:
                    await notify_user(user_id,
                        f"🔗 <b>لینک اتصال:</b>\n<code>{vpn_link}</code>\n\n"
                        f"👆 کپی و در برنامه Amnezia گزینه Import بزنید.")
                if config_text:
                    await send_user_document(user_id, f"{conn_name}.conf",
                                             config_text.encode('utf-8'),
                                             "📄 فایل کانفیگ تست رایگان")
                await notify_user_with_buttons(user_id,
                    "📱 <b>برنامه Amnezia را دانلود کنید:</b>",
                    [[{"text": "▶️ Google Play", "url": PLAY_STORE_URL},
                      {"text": "🍎 App Store", "url": APP_STORE_URL}],
                     [{"text": "❓ راهنما و رفع اشکال", "callback_data": "amzfaq"}]])
                admin_ids = [int(x.strip()) for x in os.getenv('ADMIN_CHAT_IDS', '').split(',') if x.strip()]
                await notify_many_users([(aid,
                    f"🎁 <b>تریال Amnezia جدید</b>\n👤 <code>{user_id}</code> | "
                    f"🖥 {created.get('server_name') or server_id} | 📦 {trial_gb}GB/{days}روز")
                    for aid in admin_ids])
            finally:
                await amz.close()
        except Exception as e:
            logging.error(f"provision_amnezia_trial failed for {user_id}: {e}")
            raise
    run_async(_run())



def _parse_vless_membership(inbounds):
    """PURE: from raw /panel/api/inbounds/list data, derive VLESS coverage.

    Returns (target_ids, clients) where target_ids are the ENABLED inbound ids
    whose protocol == 'vless', and clients maps every enabled vless-client
    email to {"have": set(ids-present-in), "missing": set(target-ids-lacking),
    "entry": the raw client dict}. Handles panel builds that return
    ``settings`` either as a dict or as a JSON string.
    """
    import json as _json
    target_ids = []
    clients = {}
    for ib in inbounds or []:
        if ib.get('protocol') != 'vless' or not ib.get('enable', False):
            continue
        target_ids.append(ib['id'])
        settings = ib.get('settings') or {}
        if isinstance(settings, str):
            try:
                settings = _json.loads(settings)
            except ValueError:
                settings = {}
        for cl in settings.get('clients', []) or []:
            if cl.get('enable', True) is False:
                continue   # audit scope is ENABLED vless users only
            email = cl.get('email')
            if not email:
                continue
            info = clients.setdefault(email, {"have": set(), "missing": set(),
                                              "entry": cl})
            info["have"].add(ib['id'])
    for email, info in clients.items():
        info["missing"] = set(target_ids) - info["have"]
    return target_ids, clients


@celery_app.task
def audit_vless_inbounds(admin_id: int):
    """Admin-triggered audit: ensure EVERY enabled VLESS client exists on ALL
    enabled VLESS inbounds. Missing memberships are fixed via
    delete+re-add-with-full-target-set (this panel build rejects duplicate
    emails on /clients/add — verified live), preserving traffic counters
    (keepTraffic=1), sub id, tgId, quota and expiry from the existing entry.
    Admin receives a Persian summary DM when the batch finishes."""
    async def _run():
        xui = XUIClient()
        try:
            inbounds = await xui.get_inbounds()
            target_ids, clients = _parse_vless_membership(inbounds)
            if not target_ids:
                await notify_user(admin_id, "⚠️ هیچ اینباند VLESS فعالی در پنل یافت نشد.")
                return

            complete, fixed, failures = [], [], []
            for email, info in sorted(clients.items()):
                if not info["missing"]:
                    complete.append(email)
                    continue
                try:
                    entry = info["entry"]
                    await xui.delete_client(email, keep_traffic=True)
                    await xui.add_client(
                        email=email,
                        total_bytes=entry.get('totalGB', 0),
                        expiry_time=entry.get('expiryTime', 0),
                        inbound_ids=sorted(target_ids),
                        tg_id=entry.get('tgId') or None,
                        sub_id=entry.get('subId') or None)
                    fixed.append(email)
                except Exception as e:
                    failures.append(f"{email}: {str(e)[:120]}")

            lines = [f"🔄 <b>گزارش بررسی اینباندهای VLESS</b>",
                     "━━━━━━━━━━━━━━━━━━",
                     f"👥 کل کاربران VLESS فعال: <b>{len(clients)}</b>",
                     f"✅ کامل بودند: <b>{len(complete)}</b>",
                     f"🔧 اصلاح شدند: <b>{len(fixed)}</b>"]
            if fixed[:15]:
                lines.append("🛠 نمونه: " + ", ".join(f"<code>{e}</code>" for e in fixed[:15]))
            lines.append(f"❌ خطاها: <b>{len(failures)}</b>")
            for f_ in failures[:10]:
                lines.append(f"▫️ <code>{f_}</code>")
            if not clients:
                lines.append("ℹ️ هیچ کاربر VLESS فعالی یافت نشد.")
            await notify_user(admin_id, chr(10).join(lines))
        finally:
            await xui.close()
    run_async(_run())


@celery_app.task
def check_amnezia_expiry():
    """Daily Amnezia expiry sweep: warn once ≤3 days before expiry; on expiry
    disable the service's OWN panel account, mark it expired and notify.

    Each subscription has a dedicated panel account, so disabling one service
    never touches the buyer's other services.
    """
    async def _run():
        now = datetime.now(timezone.utc)
        amz = AmneziaClient()
        try:
            notifications = []
            with SessionLocal() as db:
                services = db.query(AmneziaService).filter(
                    AmneziaService.status == 'active').all()
                for svc in services:
                    try:
                        if not svc.panel_user_id or not svc.panel_username:
                            continue
                        stats = await amz.get_user_stats(svc.panel_username)
                        exp = (stats or {}).get('expiration_date') or svc.expiry_date
                        if not exp:
                            continue
                        if exp.tzinfo is None:
                            # SQLite/edge sources may return naive datetimes
                            exp = exp.replace(tzinfo=timezone.utc)
                        days_left = (exp - now).days

                        # Trials: DELETE the whole panel account at expiry
                        # (cascade removes the connection) — this is what keeps
                        # trial entities from accumulating on the panel.
                        if svc.is_trial:
                            if exp <= now:
                                try:
                                    await amz.delete_panel_user(svc.panel_user_id)
                                except AmneziaError as e:
                                    if 'not found' not in str(e).lower():
                                        raise
                                svc.status = 'deleted'
                                notifications.append((svc.telegram_user_id,
                                    f"⏳ <b>تست رایگان Amnezia شما به پایان رسید و حذف شد.</b>\n\n"
                                    f"خوشحال می‌شویم با خرید یکی از پلن‌ها ادامه دهید 🌟"))
                            continue  # trials get no expiry warnings — short-lived by design

                        if exp <= now:
                            await amz.set_user_enabled(svc.panel_user_id, enabled=False)
                            svc.status = 'expired'
                            notifications.append((svc.telegram_user_id,
                                f"⌛ <b>سرویس Amnezia شما منقضی شد.</b>\n\n"
                                f"🔖 سرویس: <code>{svc.name}</code>\n"
                                f"برای ادامه استفاده، از منوی خرید تمدید کنید."))
                        elif days_left <= 3 and (
                                svc.expiry_warned_at is None or
                                now - svc.expiry_warned_at > timedelta(hours=20)):
                            svc.expiry_warned_at = now
                            notifications.append((svc.telegram_user_id,
                                f"⚠️ <b>هشدار اتمام سرویس Amnezia</b>\n"
                                f"━━━━━━━━━━━━━━\n"
                                f"🔖 سرویس: <code>{svc.name}</code>\n"
                                f"📅 {days_left} روز تا انقضا\n\n"
                                f"جهت جلوگیری از قطعی، لطفاً تمدید یا خرید حجم انجام دهید."))
                    except Exception as e:
                        logging.error(f"check_amnezia_expiry error for service {svc.id}: {e}")
                        continue
                db.commit()
            if notifications:
                await notify_many_users(notifications)
        finally:
            await amz.close()
    run_async(_run())


@celery_app.task
def check_amnezia_servers():
    """Hourly Amnezia server health sweep + AUTOMATIC FAILOVER.

    Panel server ids are LIST INDEXES — deleting OR REORDERING servers shifts
    every later index silently (confirmed in the panel's own source). The
    connection record fetched fresh from the panel is authoritative for both
    its current ``server_id`` and ``server_name``, so each service is
    re-anchored whenever either differs.

    On top of re-anchoring, this sweep now OWNS migration: a Redis snapshot
    (``amnezia_known_servers``) detects added/disappeared servers, and every
    active service stranded on a dead/deleted server is automatically
    re-created on a healthy target (newly-added server preferred), with the
    fresh config/vpn link DM'd to the owner. Trials migrate too. If no alive
    alternative exists, the previous warn-once behaviour applies.

    All observed (id, name) pairs rewrite the shared name cache, and the live
    set replaces the known-servers snapshot at the end of every run.
    """
    async def _run():
        now = datetime.now(timezone.utc)
        amz = AmneziaClient()
        try:
            try:
                servers = await amz.list_servers(force_refresh=True)
            except AmneziaError as e:
                logging.error(f"check_amnezia_servers: cannot list servers: {e}")
                return
            alive_by_id = {s['id']: s.get('alive', True) for s in servers}
            observed_server_names = {}

            known = await _load_known_servers()
            added_ids = [s['id'] for s in servers if s['id'] not in known]
            if added_ids:
                logging.info(f"check_amnezia_servers: new server(s) detected: {added_ids}")

            notifications = []
            admin_notes = []
            migrated_n = 0

            async def _migrate_service(svc, old_server_id, old_client_id):
                """Re-create the connection on a healthy target server and DM
                the owner the fresh config. Returns True on success."""
                target = _pick_migration_target(servers, alive_by_id,
                                                added_ids, exclude_id=old_server_id)
                if target is None:
                    return False
                target_name = next((s.get('name') for s in servers
                                    if s['id'] == target), None) or f"سرور {target}"
                new_name = f"{svc.name}m{int(time.time()) % 100000}"
                created = await amz.add_connection(svc.panel_user_id, target, new_name)
                # Re-point the service row at its new home (committed by the
                # sweep's db.commit() after the loop).
                svc.connection_id = created['connection_id']
                svc.client_id = created['client_id']
                svc.server_id = created['server_id']
                svc.server_name = created.get('server_name') or target_name
                svc.server_missing_notified_at = None
                cfg = {}
                try:
                    cfg = await amz.get_connection_config(created['server_id'],
                                                          created['client_id'])
                except AmneziaError as e:
                    logging.warning(f"migration config fetch failed for {svc.id}: {e}")
                if old_client_id:
                    try:
                        await amz.remove_connection(old_server_id, old_client_id)
                    except Exception:
                        pass  # old server may be gone entirely — nothing to clean
                vpn_link = cfg.get('vpn_link')
                body = ("🚚 <b>سرور قبلی حذف شد — سرویس شما خودکار منتقل شد!</b>\n\n"
                        f"🖥 سرور جدید: <b>{target_name}</b>\n"
                        "📦 حجم و زمان شما بدون تغییر باقی مانده است.\n\n"
                        "👇 لینک جدید را Import کنید (لینک قبلی دیگر کار نمی‌کند):")
                if vpn_link:
                    body += f"\n<code>{vpn_link}</code>"
                await notify_user_with_buttons(svc.telegram_user_id, body,
                    [[{"text": "❓ راهنما و رفع اشکال", "callback_data": "amzfaq"}]])
                if cfg.get('config'):
                    await send_user_document(svc.telegram_user_id,
                                             f"{new_name}.conf",
                                             cfg['config'].encode('utf-8'),
                                             "📄 کانفیگ جدید سرویس شما")
                return True

            with SessionLocal() as db:
                services = db.query(AmneziaService).filter(
                    AmneziaService.status == 'active').all()

                for svc in services:
                    try:
                        if not svc.panel_user_id:
                            continue
                        try:
                            conns = await amz.get_user_connections(svc.panel_user_id)
                        except AmneziaError as e:
                            logging.error(f"check_amnezia_servers: connections fetch failed "
                                          f"for service {svc.id} account {svc.panel_username}: {e}")
                            continue
                        tg_id = svc.telegram_user_id
                        conns_by_id = {c.get('id'): c for c in conns}
                        conn = conns_by_id.get(svc.connection_id)

                        # 1) Connection vanished from the panel entirely.
                        #    Auto-failover first: the panel ACCOUNT survives even
                        #    when its server/connection was deleted. No old
                        #    client_id passed — nothing left to remove.
                        if conn is None:
                            if svc.panel_user_id and \
                                    await _migrate_service(svc, svc.server_id, None):
                                migrated_n += 1
                                continue
                            if (not svc.server_missing_notified_at or
                                    now - svc.server_missing_notified_at > timedelta(hours=24)):
                                svc.server_missing_notified_at = now
                                notifications.append((tg_id,
                                    f"⚠️ <b>سرویس Amnezia شما در پنل یافت نشد.</b>\n\n"
                                    f"🔖 سرویس: <code>{svc.name}</code>\n"
                                    f"این سرویس از پنل حذف شده است. لطفاً با پشتیبانی تماس بگیرید."))
                            continue

                        # Remember every (id, name) pair seen live this run.
                        if conn.get('server_name'):
                            observed_server_names[conn.get('server_id')] = conn['server_name']

                        # 2) Re-anchor from the panel's own record.
                        cur_sid = conn.get('server_id')
                        cur_name = conn.get('server_name')
                        id_changed = cur_sid is not None and cur_sid != svc.server_id
                        name_changed = bool(cur_name and svc.server_name and
                                            cur_name != svc.server_name)
                        if id_changed:
                            old_sid = svc.server_id
                            svc.server_id = cur_sid
                            logging.info(f"check_amnezia_servers: service {svc.id} "
                                         f"re-anchored {old_sid} -> {cur_sid} "
                                         f"({'move' if name_changed else 'reorder'})")
                        if name_changed:
                            old_name = svc.server_name
                            svc.server_name = cur_name
                            notifications.append((tg_id,
                                f"ℹ️ <b>سرور سرویس Amnezia شما تغییر کرد</b>\n\n"
                                f"🔖 سرویس: <code>{svc.name}</code>\n"
                                f"🖥 {old_name} ← اکنون روی <b>{cur_name}</b>"))
                        elif id_changed and not svc.server_name and cur_name:
                            svc.server_name = cur_name

                        sid = svc.server_id
                        label = svc.server_name or f"سرور {sid}"
                        alive = alive_by_id.get(sid)

                        # 3a) Dead/deleted server WITH a healthy target -> migrate.
                        if alive is None or alive is False:
                            if await _migrate_service(svc, sid, svc.client_id):
                                migrated_n += 1
                                continue
                            # 3b) No alternative — warn once per episode.
                            if (not svc.server_missing_notified_at or
                                    now - svc.server_missing_notified_at > timedelta(hours=24)):
                                svc.server_missing_notified_at = now
                                reason = ("حذف شده" if alive is None else "در دسترس نیست")
                                notifications.append((tg_id,
                                    f"🚨 <b>سرور سرویس Amnezia شما {reason} است!</b>\n\n"
                                    f"🔖 سرویس: <code>{svc.name}</code>\n"
                                    f"🖥 سرور: <b>{label}</b>\n\n"
                                    f"تا رفع مشکل، این سرویس کار نخواهد کرد. برای انتقال به سرور دیگر با پشتیبانی تماس بگیرید."))
                        elif svc.server_missing_notified_at:
                            svc.server_missing_notified_at = None
                            notifications.append((tg_id,
                                f"✅ <b>سرور سرویس Amnezia شما دوباره آنلاین شد.</b>\n\n"
                                f"🔖 سرویس: <code>{svc.name}</code>\n"
                                f"🖥 سرور: <b>{label}</b>"))
                    except Exception as e:
                        logging.error(f"check_amnezia_servers error for service {svc.id}: {e}")
                        continue
                db.commit()

            # Rewrite caches from live observations + persist the known-set.
            merged_names = dict(await _load_known_servers())
            merged_names.update(observed_server_names)
            merged_names.update({s['id']: s.get('name') or '' for s in servers})
            await _save_known_servers(merged_names)

            if added_ids:
                names = [next((s.get('name') for s in servers if s['id'] == i), f"سرور {i}")
                         for i in added_ids]
                admin_notes = admin_notes  # noqa: F841 (kept for clarity)
                await notify_many_users([(aid,
                    "🆕 <b>سرور جدید Amnezia شناسایی شد:</b> " +
                    ", ".join(str(n) for n in names))
                    for aid in _admin_ids()])
            if migrated_n:
                await notify_many_users([(aid,
                    f"🚚 <b>جابجایی خودکار:</b> {migrated_n} سرویس به سرور سالم منتقل شد.")
                    for aid in _admin_ids()])
            if notifications:
                await notify_many_users(notifications)
        finally:
            await amz.close()
    run_async(_run())


@celery_app.task
def system_heartbeat():
    """Every 10 minutes: verify the bot's heartbeat key and DB liveness.

    Silent when everything is healthy (a daily digest proves the worker
    itself is alive); alerts admins with cooldown when the bot process or
    the database looks down, and sends a recovery notice when they return.
    """
    async def _run():
        import redis.asyncio as aioredis
        now = datetime.now(timezone.utc)
        r = aioredis.from_url(REDIS_URL, decode_responses=True)
        try:
            # 1) Bot process liveness
            raw = await r.get("bot:heartbeat")
            bot_age = None
            if raw:
                try:
                    bot_age = (now - datetime.fromisoformat(raw)).total_seconds()
                except ValueError:
                    pass
            if raw is None or (bot_age is not None and bot_age > 300):
                ok, suppressed = should_alert("bot-down", cooldown_seconds=1800)
                if ok:
                    await r.set("alert:bot-down", "1", ex=7200)
                    await notify_many_users([(aid,
                        f"🚨 <b>پروسه ربات بی‌پاسخ شد!</b>\n\n"
                        f"آخرین تپ: <code>{'هرگز' if raw is None else f'{int(bot_age)} ثانیه پیش'}</code>\n"
                        f"ورکر فعال است اما ربات پاسخ نمی‌دهد. لاگ‌های کانتینر bot را بررسی کنید.")
                        for aid in _admin_ids()])
            elif await r.exists("alert:bot-down"):
                await r.delete("alert:bot-down")
                await notify_many_users([(aid,
                    "✅ <b>ربات دوباره پاسخ داد.</b>\nمانیتور بات‌داون رفع شد.")
                    for aid in _admin_ids()])

            # 2) Database liveness
            try:
                with SessionLocal() as db:
                    db.execute(text("SELECT 1"))
            except Exception as e:
                ok, suppressed = should_alert(f"db-down:{str(e)[:80]}", cooldown_seconds=1800)
                if ok:
                    await notify_many_users([(aid, format_alert("اتصال دیتابیس", e))
                                             for aid in _admin_ids()])
        finally:
            await r.aclose()
    run_async(_run())


@celery_app.task
def daily_status_digest():
    """Daily 21:00 proof-of-life summary. If THIS message stops arriving,
    the worker (or its beat scheduler) is down — absence is the alarm."""
    async def _run():
        import redis.asyncio as aioredis
        now = datetime.now(timezone.utc)
        r = aioredis.from_url(REDIS_URL, decode_responses=True)
        try:
            raw = await r.get("bot:heartbeat")
            bot_age = None
            if raw:
                try:
                    bot_age = int((now - datetime.fromisoformat(raw)).total_seconds())
                except ValueError:
                    pass
            queue_len = await r.llen("celery")
            db_ok = True
            try:
                with SessionLocal() as db:
                    db.execute(text("SELECT 1"))
            except Exception:
                db_ok = False
            msg = (
                "📋 <b>گزارش روزانه سلامت سیستم</b>\n"
                "━━━━━━━━━━━━━━━━━━\n"
                f"🤖 ربات: {'✅ فعال' if bot_age is not None and bot_age <= 300 else '🔴 بی‌پاسخ'}"
                + (f" (آخرین تپ {bot_age} ثانیه پیش)" if bot_age is not None else "") + "\n"
                f"⚙️ ورکر: ✅ فعال (این پیام)\n"
                f"🗄 دیتابیس: {'✅' if db_ok else '🔴 خطا'}\n"
                f"📬 صف وظایف: {queue_len} مورد در انتظار\n"
                f"🕐 {now.strftime('%Y-%m-%d %H:%M')} UTC"
            )
            await notify_many_users([(aid, msg) for aid in _admin_ids()])
        finally:
            await r.aclose()
    run_async(_run())
