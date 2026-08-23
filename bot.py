import os
import re
import json
import time
import asyncio
import secrets
import logging
from typing import List
from datetime import datetime, timedelta
from aiogram import Bot, Dispatcher, F, types
from aiogram.filters import Command
from aiogram.exceptions import TelegramBadRequest
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton, BotCommand, BufferedInputFile
from database import SessionLocal, Plan, Invoice, AppSetting, TrialUsage, Coupon, CouponUsage, ReferralCode, Referral, Reseller, ResellerPack, PanelTraffic, TrafficPack, AmneziaUser, AmneziaService
from sqlalchemy import func
import redis.asyncio as redis
from xui_client import XUIClient, get_xui_client
import tasks
from datetime import timezone
from dotenv import load_dotenv

# Load environment variables (config.py also does this, but we keep it for safety)
load_dotenv()

# Setup logging configuration
from src.utils.logging_config import setup_logging
setup_logging(log_level=os.getenv('LOG_LEVEL', 'INFO'), log_file='logs/bot.log')

logger = logging.getLogger(__name__)

# Import config and utilities from src modules
from src.config import BOT_TOKEN, REQUIRED_CHANNEL_ID, REQUIRED_CHANNEL_LINK, get_admin_ids, amnezia_visible, AMNEZIA_ENABLED
from src.utils.formatting import format_size, format_price, get_progress_bar, format_expiry_remaining
from src.services.amnezia import AmneziaClient, AmneziaError, parse_expiration, PLAY_STORE_URL, APP_STORE_URL
from src.utils.keyboard import (
    get_cancel_kb, get_back_kb, get_main_menu, get_admin_menu, get_reseller_menu,
    get_admin_category_kb, get_join_prompt_kb, JOIN_PROMPT_TEXT
)
from src.services.reseller import is_reseller, get_reseller_balance, reserve_reseller_allowance, reseller_owns_email, user_owns_email, GB
from src.services.coupon import validate_coupon, calculate_discount
from src.services.reconcile import compute_reconcile, apply_plan, to_plan

# Environment variables are validated in src.config
REQ_CHANNEL_ID = REQUIRED_CHANNEL_ID
REQ_CHANNEL_LINK = REQUIRED_CHANNEL_LINK

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()
redis_client = redis.from_url(os.getenv('REDIS_URL'), decode_responses=True)

async def invalidate_user_service_cache(user_id: int):
    """Invalidate cached service list and status for a user."""
    tg_id = str(user_id)
    await redis_client.delete(f"service_status:{tg_id}")
    await redis_client.delete(f"user_emails:{tg_id}")

# ========== Global error alerting ==========
# Any unhandled exception while processing an update is logged AND sent to
# admins as a rate-limited DM, so failures surface as tickets instead of
# living only in container logs.
#
# CONTRACT (aiogram 3.x): the errors observer passes ONE ErrorEvent object.
# Declaring (event, exception) parameters instead crashes inside this very
# handler and silently kills alert delivery — locked by tests/test_error_handler.py.
from aiogram.types import ErrorEvent
from src.utils.alerting import should_alert, is_noise, format_alert

@dp.errors()
async def global_error_handler(event: ErrorEvent):
    exception = event.exception
    logger.error("Unhandled error processing update", exc_info=exception)
    if is_noise(exception):
        return True
    sig = f"{type(exception).__name__}:{str(exception)[:150]}"
    alert_now, suppressed = should_alert(f"bot:{sig}")
    if not alert_now:
        return True

    ctx = None
    try:
        upd = event.update
        if getattr(upd, 'message', None):
            m = upd.message
            ctx = f"پیام از <code>{m.chat.id}</code>"
            if m.text:
                ctx += f" — «{m.text[:60]}»"
        elif getattr(upd, 'callback_query', None):
            cq = upd.callback_query
            ctx = f"دکمه <code>{cq.data}</code> از <code>{cq.from_user.id}</code>"
    except Exception:
        ctx = None
    alert = format_alert("پردازش آپدیت تلگرام", exception, context=ctx,
                         suppressed=suppressed)
    for aid in get_admin_ids():
        try:
            await bot.send_message(aid, alert, parse_mode="HTML")
        except Exception as e:
            logger.warning(f"Failed to deliver admin alert to {aid}: {e}")
    return True

# ========== FSM States ==========
class BuyFlow(StatesGroup):
    wait_for_name = State()
    wait_for_coupon = State()
    wait_for_receipt = State()
    wait_for_amz_account = State()   # optional Amnezia panel username/password

class AddPlanFlow(StatesGroup):
    wait_for_name = State()
    wait_for_gb = State()
    wait_for_days = State()
    wait_for_price = State()

class TopupFlow(StatesGroup):
    wait_for_gb = State()
    wait_for_coupon = State()
    wait_for_receipt = State()

class AdminFlow(StatesGroup):
    wait_for_reject_reason = State()
    wait_for_inbounds = State()
    wait_for_card = State()
    wait_for_sub_link = State()
    wait_for_manage_email = State()
    wait_for_reconcile_user = State()         # per-user "repair invisible services" quick-fix
    wait_for_sync_group = State()
    wait_for_support_account = State()
    # Coupon
    wait_for_coupon_code = State()
    wait_for_coupon_toggle = State()          # separate state for toggle
    wait_for_coupon_type = State()
    wait_for_coupon_discount = State()
    wait_for_coupon_max_total = State()
    wait_for_coupon_max_per_user = State()
    wait_for_coupon_expiry = State()
    wait_for_coupon_applicable = State()
    # Trial
    wait_for_trial_traffic = State()
    wait_for_trial_days = State()
    # Referral
    wait_for_referral_threshold = State()
    wait_for_referral_reward_plan = State()
    # Billing
    wait_for_billing_date_range = State()
    # Reseller management
    wait_for_reseller_id = State()
    wait_for_reseller_allowance = State()
    wait_for_reseller_pack_days = State()     # after allowance, ask for expiry days
    wait_for_reseller_duration = State()
    wait_for_reseller_inbounds = State()
    # Broadcast
    wait_for_broadcast_message = State()
    # Dashboard
    wait_for_select_inbound = State()
    # Custom receipt
    wait_for_custom_receipt_target = State()
    wait_for_custom_receipt_amount = State()
    wait_for_custom_receipt_description = State()
    wait_for_custom_receipt_photo = State()
    wait_for_custom_receipt_service_update = State()
    wait_for_custom_receipt_service_client = State()
    wait_for_custom_receipt_service_gb = State()
    wait_for_custom_receipt_service_days = State()
    # Traffic pack management
    wait_for_pack_name = State()
    wait_for_pack_gb = State()
    wait_for_pack_price = State()
    wait_for_pack_days = State()
    wait_for_pack_delete_confirm = State()

class TrialFlow(StatesGroup):
    wait_for_name = State()

class ResellerFlow(StatesGroup):
    wait_for_name = State()
    wait_for_gb = State()
    wait_for_topup_gb = State()
    wait_for_renew_gb = State()
    wait_for_extend_days = State()

class CustomPayFlow(StatesGroup):
    wait_for_receipt = State()

class EditPlanFlow(StatesGroup):
    wait_for_name = State()
    wait_for_gb = State()
    wait_for_days = State()
    wait_for_price = State()

class ResellerPackFlow(StatesGroup):
    wait_for_coupon = State()
    wait_for_receipt = State()




async def animate_delete_message(bot: Bot, chat_id: int, message_id: int):
    """Animate a message with loading frames and delete it."""
    frames = ["⏳", "⌛", "⏳", "⌛", "🗑️"]
    try:
        for frame in frames:
            await bot.edit_message_text(frame, chat_id, message_id)
            await asyncio.sleep(0.12)
    except Exception as e:
        logger.debug(f"Animation edit failed: {e}")
    try:
        await bot.delete_message(chat_id, message_id)
    except Exception as e:
        logger.debug(f"Message deletion failed: {e}")

async def cleanup_prev_message(bot: Bot, state: FSMContext, chat_id: int):
    """Animate and delete the previous bot message stored in FSM state."""
    data = await state.get_data()
    prev_id = data.get('last_bot_msg_id')
    if prev_id:
        await animate_delete_message(bot, chat_id, prev_id)
        await state.update_data(last_bot_msg_id=None)

async def delete_user_message(bot: Bot, message: types.Message):
    """Delete a user's message safely."""
    try:
        await message.delete()
    except Exception as e:
        logger.debug(f"User message deletion failed: {e}")


async def check_membership(user_id: int) -> bool:
    cache_key = f"member_cache:{user_id}"
    if await redis_client.get(cache_key): return True
    try:
        member = await bot.get_chat_member(REQ_CHANNEL_ID, user_id)
        if member.status in ['member', 'administrator', 'creator']:
            await redis_client.set(cache_key, "1", ex=300)
            return True
    except TelegramBadRequest as e:
        # Bot lacks admin access or user not found → legitimate non-member
        logger.warning(f"Membership check failed for {user_id}: {e}")
        return False
    except Exception as e:
        # Network/transient errors → fail open to avoid blocking legitimate users
        logger.error(f"Membership check error for {user_id}: {e}")
        return True
    return False


async def can_use_trial(user_id: int) -> bool:
    with SessionLocal() as db:
        trial = db.query(TrialUsage).filter(TrialUsage.telegram_user_id == user_id).first()
        if not trial:
            return True
        return (datetime.now(timezone.utc) - trial.last_trial_date).days >= 30

def generate_referral_code(user_id: int) -> str:
    return f"ref_{user_id}_{secrets.token_hex(3)}"

async def ensure_referral_code(user_id: int):
    with SessionLocal() as db:
        rec = db.query(ReferralCode).filter(ReferralCode.telegram_user_id == user_id).first()
        if not rec:
            code = generate_referral_code(user_id)
            db.add(ReferralCode(telegram_user_id=user_id, code=code))
            db.commit()



def get_all_user_ids() -> List[int]:
    """Distinct Telegram IDs of everyone who has used the bot.
    Includes ReferralCode, Invoice, and TrialUsage."""
    with SessionLocal() as db:
        ids = {row[0] for row in db.query(ReferralCode.telegram_user_id).all()}
        ids.update(row[0] for row in db.query(Invoice.telegram_user_id).distinct().all())
        ids.update(row[0] for row in db.query(TrialUsage.telegram_user_id).distinct().all())
    return [i for i in ids if i]

# ==============================================================================
# ENTRY POINTS
# ==============================================================================

async def build_welcome_message(user_id: int, ref_code: str = None) -> str:
    """Ensure referral code, record any incoming referral, and build the welcome text.
    Shared by /start and the 'I joined' callback so both paths behave identically."""
    await ensure_referral_code(user_id)
    ref_bonus = ""
    if ref_code and ref_code.startswith('ref_'):
        with SessionLocal() as db:
            existing = db.query(Referral).filter(Referral.referred_user_id == user_id).first()
            if not existing:
                referrer = db.query(ReferralCode).filter(ReferralCode.code == ref_code).first()
                if referrer and referrer.telegram_user_id != user_id:
                    db.add(Referral(referrer_id=referrer.telegram_user_id, referred_user_id=user_id))
                    db.commit()
                    ref_bonus = "\n\n🎉 <b>با تشکر از دوستتان!</b> شما با لینک دعوت وارد شدید."
    with SessionLocal() as db:
        shop_setting = db.query(AppSetting).filter(AppSetting.key == 'shop_name').first()
        shop_name = shop_setting.value if shop_setting else "فروشگاه رهانت"
    return (
        f"🌟 <b>به {shop_name} خوش آمدید!</b> 🌟\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"🚀 تجربه اینترنتی سریع، امن و بدون محدودیت با پروتکل‌های مدرن.\n"
        f"💡 ما اینجا هستیم تا بهترین سرویس را به شما ارائه دهیم.\n"
        f"{ref_bonus}\n\n"
        f"👇 <i>لطفاً از منوی زیر گزینه مورد نظر خود را انتخاب کنید:</i>"
    )

@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    if not await check_membership(message.from_user.id):
        # Stash any referral payload so it survives the join → "I joined" round-trip
        args = message.text.split()
        if len(args) > 1 and args[1].startswith('ref_'):
            await redis_client.set(f"pending_ref:{message.from_user.id}", args[1], ex=3600)
        return await message.answer(JOIN_PROMPT_TEXT, reply_markup=get_join_prompt_kb(REQ_CHANNEL_LINK), parse_mode="HTML")

    args = message.text.split()
    ref_code = args[1] if len(args) > 1 else None
    msg = await build_welcome_message(message.from_user.id, ref_code)
    await message.answer(msg, reply_markup=get_main_menu(message.from_user.id), parse_mode="HTML")

@dp.callback_query(F.data == "check_join")
async def check_join_cb(callback: types.CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    if not await check_membership(user_id):
        return await callback.answer(
            "⚠️ هنوز عضویت شما تأیید نشد. ابتدا در کانال عضو شوید، سپس دوباره روی «عضو شدم» بزنید.",
            show_alert=True
        )
    # Member confirmed — pull any stashed referral payload from the /start deep link
    ref_code = await redis_client.get(f"pending_ref:{user_id}")
    if ref_code:
        await redis_client.delete(f"pending_ref:{user_id}")
    msg = await build_welcome_message(user_id, ref_code)
    try:
        await callback.answer("✅ عضویت شما تأیید شد!")
    except Exception as e:
        logger.debug(f"Callback answer failed: {e}")
    try:
        await callback.message.delete()
    except Exception as e:
        logger.debug(f"Callback message deletion failed: {e}")
    await bot.send_message(user_id, msg, reply_markup=get_main_menu(user_id), parse_mode="HTML")

@dp.message(Command("menu"))
async def cmd_menu(message: types.Message, state: FSMContext):
    await state.clear()
    if not await check_membership(message.from_user.id): return await cmd_start(message)
    await message.answer("🏠 <b>منوی اصلی:</b>", reply_markup=get_main_menu(message.from_user.id), parse_mode="HTML")

@dp.callback_query(F.data == "cancel")
async def cancel_handler(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if "invoice_id" in data:
        with SessionLocal() as db:
            # Atomic unlock: only revert LOCKED → PENDING if still LOCKED
            rows = db.query(Invoice).filter(
                Invoice.id == int(data["invoice_id"]),
                Invoice.status == "LOCKED"
            ).update({"status": "PENDING"})
            db.commit()
    await state.clear()
    try:
        await callback.answer()
    except Exception as e:
        logger.debug(f"Cancel callback answer failed: {e}")
    is_admin = callback.from_user.id in get_admin_ids()
    if is_admin:
        msg = "🚫 <b>عملیات لغو شد.</b>"
    else:
        msg = "🚫 <b>عملیات لغو شد.</b>\n\n🙏 اگر سوالی دارید، خوشحال می‌شویم کمک کنیم."
    await callback.message.answer(msg, reply_markup=get_main_menu(callback.from_user.id), parse_mode="HTML")

@dp.callback_query(F.data == "main_menu")
async def back_to_main(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    try:
        await callback.answer()
    except Exception as e:
        logger.debug(f"Main menu callback answer failed: {e}")
    # Send a new message with the reply keyboard (can't use edit_text)
    await callback.message.answer("🏠 <b>منوی اصلی:</b>", reply_markup=get_main_menu(callback.from_user.id), parse_mode="HTML")

# ========== Reply Keyboard (Main Menu) Message Handlers ==========
@dp.message(F.text == "🛒 خرید اشتراک جدید")
async def mm_buy_plan(message: types.Message, state: FSMContext):
    if not await check_membership(message.from_user.id):
        return await cmd_start(message)
    text, kb = await show_plans_content(state, user_id=message.from_user.id)
    await message.answer(text, reply_markup=kb, parse_mode="HTML")

@dp.message(F.text == "🟣 خرید Amnezia")
async def mm_buy_amnezia(message: types.Message, state: FSMContext):
    if not await check_membership(message.from_user.id):
        return await cmd_start(message)
    if not amnezia_visible(message.from_user.id):
        return await message.answer("⛔ این بخش در دسترس شما نیست.")
    text, kb = await show_plans_content(state, user_id=message.from_user.id, amnezia_only=True)
    await message.answer(text, reply_markup=kb, parse_mode="HTML")

@dp.message(F.text == "📦 سرویس‌های من")
async def mm_my_plans(message: types.Message):
    if not await check_membership(message.from_user.id):
        return await cmd_start(message)
    text, kb = await my_plans_content(message.from_user.id)
    await message.answer(text, reply_markup=kb, parse_mode="HTML")

@dp.message(F.text == "🎁 تست رایگان")
async def mm_free_trial(message: types.Message, state: FSMContext):
    if not await check_membership(message.from_user.id):
        return await cmd_start(message)
    if not await can_use_trial(message.from_user.id):
        return await message.answer(
            "😊 <b>شما قبلاً از تست رایگان استفاده کرده‌اید.</b>\n\n"
            "🔄 هر ۳۰ روز یکبار می‌توانید مجدداً درخواست دهید.\n"
            "💡 در ضمن، می‌توانید از اشتراک‌های ویژه ما استفاده کنید!",
            parse_mode="HTML"
        )
    text, kb = await free_trial_content()
    await message.answer(text, reply_markup=kb, parse_mode="HTML")
    await state.set_state(TrialFlow.wait_for_name)

@dp.message(F.text == "🤝 دعوت از دوستان")
async def mm_referral_info(message: types.Message):
    if not await check_membership(message.from_user.id):
        return await cmd_start(message)
    text, kb = await referral_info_content(message.from_user.id)
    await message.answer(text, parse_mode="HTML", reply_markup=kb)

@dp.message(F.text == "🎧 پشتیبانی")
async def mm_support(message: types.Message):
    with SessionLocal() as db:
        support_setting = db.query(AppSetting).filter(AppSetting.key == 'support_url').first()
    support_url = support_setting.value if support_setting else "https://t.me/your_support"
    await message.answer(
        "🎧 <b>پشتیبانی</b>\n\n"
        "🙋 ما اینجا هستیم تا به شما کمک کنیم.\n"
        "قبل از پیام دادن، پاسخ سوالتان شاید همین‌جا باشد:\n",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❓ راهنمای Amnezia و رفع اشکال", callback_data="amzfaq")],
            [InlineKeyboardButton(text="📨 ارتباط با پشتیبانی", url=support_url)],
            [InlineKeyboardButton(text="🏠 منوی اصلی", callback_data="main_menu")]
        ]),
        parse_mode="HTML"
    )

@dp.message(F.text == "📜 تاریخچه خرید")
async def mm_purchase_history(message: types.Message):
    if not await check_membership(message.from_user.id):
        return await cmd_start(message)
    # Create a fake callback query to reuse the user_logs_callback logic
    class FakeCallback:
        def __init__(self, msg):
            self.from_user = msg.from_user
            self.message = msg
        async def message_edit_text(self, text, reply_markup=None, parse_mode=None):
            await self.message.answer(text, reply_markup=reply_markup, parse_mode=parse_mode)
    
    fake_callback = FakeCallback(message)
    await user_logs_callback(fake_callback)

@dp.message(F.text == "⚙️ پنل مدیریت")
async def mm_admin_panel(message: types.Message):
    if message.from_user.id not in get_admin_ids():
        return
    await message.answer("⚙️ <b>پنل مدیریت</b> 👑", reply_markup=get_admin_menu(), parse_mode="HTML")

@dp.message(F.text == "🧑‍💼 پنل نمایندگی")
async def mm_reseller_panel(message: types.Message):
    if not is_reseller(message.from_user.id):
        return
    await message.answer("🧑‍💼 <b>پنل نمایندگی</b> 📊", reply_markup=get_reseller_menu(), parse_mode="HTML")

# ==============================================================================
# REFERRAL INFO & CLAIM
# ==============================================================================
async def referral_info_content(user_id: int) -> tuple:
    """Shared logic for referral info. Returns (text, reply_markup)."""
    await ensure_referral_code(user_id)
    with SessionLocal() as db:
        rec = db.query(ReferralCode).filter(ReferralCode.telegram_user_id == user_id).first()
        code = rec.code if rec else ""
        count = db.query(Referral).filter(Referral.referrer_id == user_id, Referral.became_paid == True).count()
        threshold_setting = db.query(AppSetting).filter(AppSetting.key == 'referral_threshold').first()
        threshold = int(threshold_setting.value) if threshold_setting else 10
        reward_plan_id = db.query(AppSetting).filter(AppSetting.key == 'referral_reward_plan_id').first()
        reward_plan = None
        if reward_plan_id:
            reward_plan = db.query(Plan).filter(Plan.id == int(reward_plan_id.value)).first()
    
    remaining = max(0, threshold - count)
    progress = get_progress_bar(count, threshold, 8) if threshold > 0 else ""
    
    bot_me = await bot.get_me()
    text = (
        f"🤝 <b>برنامه معرفی و دعوت از دوستان</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"هر دوست که از طریق لینک اختصاصی شما ثبت‌نام کند و سرویس بخرد،"
        f" یک امتیاز برای شما محسوب می‌شود!\n\n"
        f"🎯 <b>آستانه جایزه:</b> دعوت {threshold} نفر\n"
        f"🎁 <b>پلن جایزه:</b> {reward_plan.name if reward_plan else 'تنظیم نشده'}\n"
        f"\n"
        f"📊 <b>پیشرفت شما:</b>\n"
        f"{progress}\n"
        f"{'⭐' * min(count, 10)} {count} از {threshold} دعوت موفق\n"
    )
    
    if count < threshold:
        text += f"📌 <b>{remaining} دعوت</b> دیگر تا دریافت جایزه باقی مانده!\n\n"
    else:
        text += "\n🎉 <b>تبریک! به آستانه مورد نظر رسیده‌اید!</b>\n\n"
    
    text += (
        f"🔗 <b>لینک اختصاصی شما:</b>\n"
        f"<code>https://t.me/{bot_me.username}?start={code}</code>\n\n"
        f"💡 <i>این لینک را برای دوستان خود ارسال کنید. هر خرید جدید امتیاز شما را افزایش می‌دهد!</i>"
    )

    if count >= threshold and not rec.reward_claimed:
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🎁 دریافت جایزه", callback_data="claim_reward")],
            [InlineKeyboardButton(text="🏠 منوی اصلی", callback_data="main_menu")]
        ])
    else:
        with SessionLocal() as db:
            support_setting = db.query(AppSetting).filter(AppSetting.key == 'support_url').first()
        support_url = support_setting.value if support_setting else "https://t.me/your_support"
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📋 نحوه دریافت دعوتنامه", url=support_url)],
            [InlineKeyboardButton(text="🏠 منوی اصلی", callback_data="main_menu")]
        ])
    return text, kb

@dp.callback_query(F.data == "referral_info")
async def referral_info_cb(callback: types.CallbackQuery):
    text, kb = await referral_info_content(callback.from_user.id)
    await callback.message.edit_text(text, parse_mode="HTML", reply_markup=kb)

@dp.callback_query(F.data == "claim_reward")
async def claim_reward(callback: types.CallbackQuery):
    with SessionLocal() as db:
        rec = db.query(ReferralCode).filter(ReferralCode.telegram_user_id == callback.from_user.id).first()
        if not rec or rec.reward_claimed:
            await callback.answer("شما قبلاً جایزه خود را دریافت کرده‌اید.", show_alert=True)
            return
        count = db.query(Referral).filter(Referral.referrer_id == callback.from_user.id, Referral.became_paid == True).count()
        threshold_setting = db.query(AppSetting).filter(AppSetting.key == 'referral_threshold').first()
        threshold = int(threshold_setting.value) if threshold_setting else 10
        if count < threshold:
            await callback.answer(f"شما هنوز به {threshold} دعوت موفق نرسیده‌اید.", show_alert=True)
            return
        reward_plan_id = db.query(AppSetting).filter(AppSetting.key == 'referral_reward_plan_id').first()
        if not reward_plan_id:
            await callback.answer("پلن جایزه تنظیم نشده است. به مدیریت اطلاع دهید.", show_alert=True)
            return
        plan = db.query(Plan).filter(Plan.id == int(reward_plan_id.value)).first()
        if not plan:
            await callback.answer("پلن جایزه معتبر نیست.", show_alert=True)
            return
        
        plan_id = plan.id
        rec.reward_claimed = True
        db.commit()
    
    tasks.provide_referral_reward.delay(callback.from_user.id, plan_id)
    try:
        await callback.answer()
    except Exception as e:
        logger.debug(f"Referral reward callback answer failed: {e}")
    await callback.message.answer("🎉 <b>درخواست جایزه ثبت شد!</b> 🌟\nسرویس رایگان شما در حال آماده‌سازی است. چند لحظه بعد از منوی «📦 سرویس‌های من» قابل مشاهده خواهد بود.\n\n🙏 از همراهی شما سپاسگزاریم!", reply_markup=get_main_menu(callback.from_user.id), parse_mode="HTML")

# ==============================================================================
# FREE TRIAL
# ==============================================================================
async def free_trial_content() -> tuple:
    """Shared logic for free trial text. Returns (text, reply_markup)."""
    with SessionLocal() as db:
        traffic_setting = db.query(AppSetting).filter(AppSetting.key == 'trial_traffic_gb').first()
        days_setting = db.query(AppSetting).filter(AppSetting.key == 'trial_duration_days').first()
        traffic = float(traffic_setting.value) if traffic_setting else 0.1
        days = int(days_setting.value) if days_setting else 1
    
    traffic_display = f"{traffic:.1f}".rstrip('0').rstrip('.') if traffic < 1 else str(int(traffic))
    
    text = (
        f"🎁 <b>تست رایگان</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"شما می‌توانید یک سرویس تست رایگان به مدت <b>{days} روز</b> با حجم <b>{traffic_display} گیگابایت</b> دریافت کنید.\n\n"
        f"📝 <b>یک نام برای اتصال خود وارد کنید:</b>\n"
        f"▫️ فقط حروف انگلیسی (a-z, A-Z)\n"
        f"▫️ مثال: <code>ali</code> یا <code>myVPN</code>"
    )
    return text, get_cancel_kb()

@dp.callback_query(F.data == "free_trial")
async def free_trial_start_cb(callback: types.CallbackQuery, state: FSMContext):
    if not await can_use_trial(callback.from_user.id):
        await callback.answer(
            "⚠️ شما قبلاً از تست رایگان استفاده کرده‌اید.\n\n"
            "هر ۳۰ روز یکبار می‌توانید مجدداً درخواست دهید.",
            show_alert=True
        )
        return
    text, kb = await free_trial_content()
    await callback.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    await state.update_data(last_bot_msg_id=callback.message.message_id)
    await state.set_state(TrialFlow.wait_for_name)

@dp.message(TrialFlow.wait_for_name)
async def trial_name(message: types.Message, state: FSMContext):
    if not re.match(r"^[A-Za-z]+$", message.text):
        return await message.answer(
            "⚠️ <b>فرمت نامعتبر.</b>\n\n"
            "نام اتصال باید:\n"
            "▫️ فقط شامل حروف انگلیسی (a-z, A-Z) باشد\n"
            "▫️ مثال: <code>ali</code> یا <code>myVPN</code>",
            reply_markup=get_cancel_kb(), parse_mode="HTML"
        )
    
    with SessionLocal() as db:
        traffic_setting = db.query(AppSetting).filter(AppSetting.key == 'trial_traffic_gb').first()
        days_setting = db.query(AppSetting).filter(AppSetting.key == 'trial_duration_days').first()
        traffic = float(traffic_setting.value) if traffic_setting else 0.1
        days = int(days_setting.value) if days_setting else 1
    
    traffic_display = f"{traffic:.1f}".rstrip('0').rstrip('.') if traffic < 1 else str(int(traffic))
    
    await delete_user_message(bot, message)
    await cleanup_prev_message(bot, state, message.chat.id)
    await bot.send_chat_action(message.chat.id, "typing")
    tasks.provision_trial.delay(message.from_user.id, message.text, traffic, days)
    await invalidate_user_service_cache(message.from_user.id)
    sent = await message.answer(
        f"⏳ <b>در حال ساخت سرویس تست...</b>\n\n"
        f"📶 حجم: {traffic_display} GB\n"
        f"⏳ مدت: {days} روز\n\n"
        f"لطفاً چند لحظه صبر کنید. پس از فعال‌سازی، از منوی <b>📦 سرویس‌های من</b> قابل مشاهده خواهد بود.",
        reply_markup=get_main_menu(message.from_user.id), parse_mode="HTML"
    )
    await state.clear()

# ==============================================================================
# PURCHASE FLOW (NEW, RENEW, TOPUP)
# ==============================================================================
async def show_plans_content(state: FSMContext, user_id: int = None, amnezia_only: bool = False):
    """Shared logic for showing plans. Returns (text, reply_markup).

    Amnezia plans are listed in a separate section, visible only when
    amnezia_visible(user_id) — i.e. everyone when AMNEZIA_ENABLED is on,
    otherwise admins only (experimental mode). With ``amnezia_only`` the
    listing shows ONLY the Amnezia section (dedicated 🟣 خرید Amnezia button).
    """
    await state.update_data(action_type="NEW")
    with SessionLocal() as db:
        q = db.query(Plan).filter(Plan.is_active == True)
        if amnezia_only:
            q = q.filter(Plan.service_type == 'amnezia')
        plans = q.all()

    xui_plans = [p for p in plans if (p.service_type or 'xui') != 'amnezia']
    amnezia_plans = [p for p in plans if p.service_type == 'amnezia']
    show_amnezia = bool(amnezia_plans) and user_id is not None and amnezia_visible(user_id)

    if amnezia_only:
        if not show_amnezia:
            return (
                "🟣 <b>خرید اشتراک Amnezia</b>\n━━━━━━━━━━━━━━━━━━\n\n"
                "⚠️ در حال حاضر هیچ پلن Amnezia فعالی موجود نیست.\n"
                "لطفاً بعداً مراجعه کنید.",
                get_back_kb("main_menu")
            )
        text = "🟣 <b>پلن‌های اشتراک Amnezia:</b>\n━━━━━━━━━━━━━━━━━━\n"
        kb_buttons = []
        for p in amnezia_plans:
            gb_price = p.price / p.traffic_gb if p.traffic_gb > 0 else 0
            size_str = "♾️ نامحدود" if (p.traffic_gb or 0) == 0 else f"{p.traffic_gb} گیگابایت"
            text += (
                f"\n🟣 <b>{p.name}</b>\n"
                f"   📶 حجم: <b>{size_str}</b>\n"
                f"   ⏳ مدت: <b>{p.duration_days} روز</b>\n"
                f"   💰 قیمت: <b>{p.price:,} تومان</b>"
            )
            if gb_price > 0:
                text += f" | <i>~{gb_price:,.0f} تومان/گیگ</i>"
            text += "\n"
            kb_buttons.append([InlineKeyboardButton(text=f"🟣 {p.name} — {p.price:,} تومان", callback_data=f"amzplan_{p.id}")])
        text += "\n👇 برای خرید، یکی از پلن‌ها را انتخاب کنید:"
        kb_buttons.append([InlineKeyboardButton(text="❌ انصراف", callback_data="cancel")])
        return text, InlineKeyboardMarkup(inline_keyboard=kb_buttons)

    if not xui_plans and not show_amnezia:
        return (
            "❌ <b>متأسفانه در حال حاضر هیچ پلنی موجود نیست.</b>\n\nلطفاً بعداً مراجعه کنید.",
            get_back_kb("main_menu")
        )

    text = "🛍 <b>پلن‌های اشتراک موجود:</b>\n━━━━━━━━━━━━━━━━━━\n"
    kb_buttons = []
    for i, p in enumerate(xui_plans, 1):
        gb_price = p.price / p.traffic_gb if p.traffic_gb > 0 else 0
        size_str = "♾️ نامحدود" if (p.traffic_gb or 0) == 0 else f"{p.traffic_gb} گیگابایت"
        text += (
            f"\n{i}️⃣ <b>{p.name}</b>\n"
            f"   📶 حجم: <b>{size_str}</b>\n"
            f"   ⏳ مدت: <b>{p.duration_days} روز</b>\n"
            f"   💰 قیمت: <b>{p.price:,} تومان</b>"
        )
        if gb_price > 0:
            text += f" | <i>~{gb_price:,.0f} تومان/گیگ</i>"
        text += "\n"
        kb_buttons.append([InlineKeyboardButton(text=f"🛒 {p.name} — {p.price:,} تومان", callback_data=f"select_plan_{p.id}")])

    if show_amnezia:
        text += "\n\n🟣 <b>پلن‌های Amnezia:</b>\n━━━━━━━━━━━━━━━━━━\n"
        for p in amnezia_plans:
            gb_price = p.price / p.traffic_gb if p.traffic_gb > 0 else 0
            size_str = "♾️ نامحدود" if (p.traffic_gb or 0) == 0 else f"{p.traffic_gb} گیگابایت"
            text += (
                f"\n🟣 <b>{p.name}</b>\n"
                f"   📶 حجم: <b>{size_str}</b>\n"
                f"   ⏳ مدت: <b>{p.duration_days} روز</b>\n"
                f"   💰 قیمت: <b>{p.price:,} تومان</b>"
            )
            if gb_price > 0:
                text += f" | <i>~{gb_price:,.0f} تومان/گیگ</i>"
            text += "\n"
            kb_buttons.append([InlineKeyboardButton(text=f"🟣 {p.name} — {p.price:,} تومان", callback_data=f"amzplan_{p.id}")])

    text += "\n👇 برای خرید، یکی از پلن‌ها را انتخاب کنید:"

    kb_buttons.append([InlineKeyboardButton(text="❌ انصراف", callback_data="cancel")])
    return text, InlineKeyboardMarkup(inline_keyboard=kb_buttons)

@dp.callback_query(F.data == "buy_plan")
async def show_plans_cb(callback: types.CallbackQuery, state: FSMContext):
    text, kb = await show_plans_content(state, user_id=callback.from_user.id)
    await callback.message.edit_text(text, reply_markup=kb, parse_mode="HTML")

@dp.callback_query(F.data.startswith("select_plan_"))
async def ask_connection_name(callback: types.CallbackQuery, state: FSMContext):
    plan_id = int(callback.data.split("_")[2])
    await state.update_data(plan_id=plan_id)
    await callback.message.edit_text(
        "🛒 <b>مرحله 1 از 3: نام اتصال</b>\n\n"
        "📝 نام اتصال خود را وارد کنید:\n"
        "این نام برای شناسایی سرویس شما استفاده می‌شود.\n"
        "▫️ فقط حروف انگلیسی (a-z, A-Z)\n"
        "▫️ مثال: <code>ali</code> یا <code>myOffice</code>",
        reply_markup=get_cancel_kb(), parse_mode="HTML"
    )
    await state.set_state(BuyFlow.wait_for_name)

@dp.message(BuyFlow.wait_for_name)
async def validate_name(message: types.Message, state: FSMContext):
    if not re.match(r"^[A-Za-z]+$", message.text):
        return await message.answer("⚠️ <b>فرمت نامعتبر.</b>\nنام اتصال باید فقط شامل حروف انگلیسی (a-z, A-Z) باشد:", reply_markup=get_cancel_kb(), parse_mode="HTML")
    await state.update_data(client_name=message.text)
    # Clean up user's name message
    await delete_user_message(bot, message)
    # Send coupon question and track it
    sent = await message.answer(
        "🎫 <b>مرحله 2 از 3: کد تخفیف</b>\n\n"
        "اگر کد تخفیف دارید، آن را وارد کنید. در غیر این صورت «رد شدن» را بزنید.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⏭ رد شدن", callback_data="skip_coupon")]
        ]), parse_mode="HTML"
    )
    await state.update_data(last_bot_msg_id=sent.message_id)
    await state.set_state(BuyFlow.wait_for_coupon)

@dp.callback_query(F.data == "skip_coupon", BuyFlow.wait_for_coupon)
async def skip_coupon(callback: types.CallbackQuery, state: FSMContext):
    await state.update_data(coupon_code=None)
    await cleanup_prev_message(bot, state, callback.message.chat.id)
    await show_payment(callback.message, state)

@dp.message(BuyFlow.wait_for_coupon)
async def process_coupon(message: types.Message, state: FSMContext):
    code = message.text.strip()
    with SessionLocal() as db:
        coupon = db.query(Coupon).filter(Coupon.code == code, Coupon.active == True).first()
        if not coupon:
            await message.answer("❌ کد تخفیف معتبر نیست. لطفاً مجدداً وارد کنید یا رد شوید.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⏭ رد شدن", callback_data="skip_coupon")]]))
            return
        if coupon.expiry_date and coupon.expiry_date.replace(tzinfo=timezone.utc) < datetime.now(timezone.utc):
            await message.answer("❌ این کد تخفیف منقضی شده است.")
            return
        total_used = db.query(CouponUsage).filter(CouponUsage.coupon_id == coupon.id).count()
        if coupon.max_uses_total > 0 and total_used >= coupon.max_uses_total:
            await message.answer("❌ این کد تخفیف دیگر قابل استفاده نیست (تعداد استفاده‌ها به پایان رسیده).")
            return
        user_used = db.query(CouponUsage).filter(CouponUsage.coupon_id == coupon.id, CouponUsage.user_id == message.from_user.id).count()
        if coupon.max_uses_per_user > 0 and user_used >= coupon.max_uses_per_user:
            await message.answer("❌ شما قبلاً از این کد تخفیف استفاده کرده‌اید.")
            return
        applicable = coupon.applicable_to
        data = await state.get_data()
        # action_type is NEW/RENEW/TOPUP (optionally AMNEZIA_-prefixed);
        # coupon applicable keys are new/renewal/topup
        action = _coupon_action_key(data.get('action_type', 'NEW'))
        action_key = {'renew': 'renewal'}.get(action, action)
        if applicable != 'all':
            allowed = applicable.split(',')
            if action_key not in allowed:
                await message.answer("❌ این کد تخفیف برای این نوع خرید قابل استفاده نیست.")
                return
        await state.update_data(coupon_code=code)
    await delete_user_message(bot, message)
    await cleanup_prev_message(bot, state, message.chat.id)
    await show_payment(message, state)

async def show_payment(message: types.Message, state: FSMContext):
    data = await state.get_data()
    action = data.get('action_type', 'NEW')
    coupon_code = data.get('coupon_code')
    is_amnezia = str(action).startswith('AMNEZIA_')
    base_action = action.replace('AMNEZIA_', '') if is_amnezia else action

    with SessionLocal() as db:
        if base_action == 'NEW':
            plan = db.query(Plan).filter(Plan.id == data['plan_id']).first()
            original_price = plan.price
        elif base_action == 'RENEW':
            plan = db.query(Plan).filter(Plan.id == data['plan_id']).first()
            original_price = plan.price
        elif base_action == 'TOPUP':
            original_price = data['total_price']
        else:
            original_price = 0

        coupon = None
        if coupon_code:
            coupon = db.query(Coupon).filter(Coupon.code == coupon_code).first()

        final_price, discount, discount_desc = calculate_discount(original_price, base_action, coupon)

        card_info = db.query(AppSetting).filter(AppSetting.key == "payment_card").first()
        card_text = card_info.value if card_info else "<i>⚠️ اطلاعات پرداخت تنظیم نشده است. به مدیریت اطلاع دهید.</i>"
        size_setting = db.query(AppSetting).filter(AppSetting.key == 'max_receipt_size_mb').first()
        max_size_mb = int(size_setting.value) if size_setting else 10

    msg = (
        f"💳 <b>مرحله آخر: پرداخت فاکتور</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
    )
    if base_action == 'NEW':
        msg += f"{'🟣' if is_amnezia else '📦'} <b>پلن:</b> {plan.name}\n"
    elif base_action == 'RENEW':
        msg += f"🔄 <b>تمدید:</b> {plan.name}\n"
    elif base_action == 'TOPUP':
        msg += f"➕ <b>حجم اضافه:</b> {data['added_gb']} گیگابایت\n"
    if is_amnezia and data.get('amnezia_server_name'):
        msg += f"🖥 <b>سرور Amnezia:</b> {data['amnezia_server_name']}\n"
    
    msg += f"💰 <b>قیمت اصلی:</b> {original_price:,} تومان\n"
    if discount > 0:
        msg += f"🎫 <b>تخفیف:</b> <span class='tg-spoiler'>-{discount:,} تومان</span> ({discount_desc})\n"
    msg += f"━━━━━━━━━━━━━━━━━━\n"
    msg += f"💵 <b>مبلغ نهایی:</b> <b>{final_price if discount > 0 else original_price:,} تومان</b>\n"
    msg += f"\n"
    msg += f"📋 <b>اطلاعات پرداخت:</b>\n<blockquote>{card_text}</blockquote>\n"
    msg += (
        f"📌 <b>نکات مهم:</b>\n"
        f"▫️ پس از واریز، <b>عکس رسید</b> را ارسال کنید\n"
        f"▫️ حداکثر حجم مجاز: {max_size_mb} مگابایت\n"
        f"▫️ پس از تایید ادمین، سرویس شما فعال خواهد شد\n"
    )
    
    await state.update_data(final_price=final_price, original_price=original_price, discount_amount=discount, discount_desc=discount_desc, coupon_code=coupon_code)
    sent = await message.answer(msg, reply_markup=get_cancel_kb(), parse_mode="HTML")
    await state.update_data(last_bot_msg_id=sent.message_id)
    await state.set_state(BuyFlow.wait_for_receipt)

def _build_invoice_kwargs(data: dict) -> dict:
    """Derive every Invoice field from FSM state — PURE, no I/O.

    This is exactly the seam that once crashed production with
    ``KeyError: 'client_name'`` (an Amnezia purchase reaching the receipt
    step stamped as a plain NEW), which is why it lives in one testable
    function instead of inline kwargs.
    """
    action_type = data.get("action_type", "NEW")
    target_email = data.get("target_email", None)
    is_amnezia = str(action_type).startswith("AMNEZIA_")
    amnezia_desc = {k: v for k, v in {
        "server_id": data.get('amnezia_server_id'),
        "amz_username": data.get('amz_username'),
        "amz_password": data.get('amz_password'),
    }.items() if v is not None}
    return dict(
        plan_id=data.get('plan_id', None),
        added_gb=data.get('added_gb', None),
        total_price=data.get('final_price', 0),
        original_price=data.get('original_price', 0),
        discount_amount=data.get('discount_amount', 0),
        coupon_code=data.get('coupon_code'),
        # Amnezia names are auto-assigned at provisioning time
        # (amz_{user}_{invoice}); the chosen server and the optional panel
        # account credentials ride in the description JSON.
        client_name=(None if is_amnezia else (target_email if target_email else data.get('client_name'))),
        action_type=action_type,
        amnezia_service_id=data.get('target_service_id') if is_amnezia else None,
        description=(json.dumps(amnezia_desc) if is_amnezia and amnezia_desc else None),
    )


def _coupon_action_key(action_type: str) -> str:
    """Map an invoice action_type onto coupon ``applicable_to`` vocabulary:
    new/renewal/topup (AMNEZIA_ prefixes collapse onto their base action)."""
    base = (action_type or 'NEW').lower().replace('amnezia_', '')
    return {'renew': 'renewal'}.get(base, base)


@dp.message(BuyFlow.wait_for_receipt, F.photo)
async def process_receipt(message: types.Message, state: FSMContext):
    # Show typing indicator while processing
    await bot.send_chat_action(message.chat.id, "upload_photo")

    # Check file size
    max_size_mb = 10
    with SessionLocal() as db:
        size_setting = db.query(AppSetting).filter(AppSetting.key == 'max_receipt_size_mb').first()
        if size_setting:
            max_size_mb = int(size_setting.value)
    max_bytes = max_size_mb * 1024 * 1024
    if message.photo[-1].file_size > max_bytes:
        await message.answer(f"⚠️ حجم عکس بیش از حد مجاز ({max_size_mb} MB) است. لطفاً عکس را فشرده کنید و دوباره ارسال نمایید.", reply_markup=get_cancel_kb())
        return

    data = await state.get_data()
    photo = message.photo[-1]
    file_path = f"./storage/receipts/{message.from_user.id}_{time.time_ns()}.jpg"
    await bot.download(photo, destination=file_path)

    # Derive everything ONCE here; the rest of this handler (coupon locking,
    # admin caption) reads these locals — they used to be inline assignments
    # and were lost in the _build_invoice_kwargs extraction (NameError).
    inv_kwargs = _build_invoice_kwargs(data)
    action_type = inv_kwargs['action_type']
    is_amnezia = str(action_type).startswith("AMNEZIA_")
    coupon_code = data.get('coupon_code')
    final_price = data.get('final_price', 0)
    original_price = data.get('original_price', 0)
    discount_amount = data.get('discount_amount', 0)

    with SessionLocal() as db:
        invoice = Invoice(
            telegram_user_id=message.from_user.id,
            screenshot_local_path=file_path,
            **inv_kwargs,
        )
        db.add(invoice)
        db.commit()
        db.refresh(invoice)

        # Capture values before session closes
        invoice_id = invoice.id
        client_name = invoice.client_name

        if coupon_code:
            # Lock the coupon row to prevent race conditions
            coupon = db.query(Coupon).filter(Coupon.code == coupon_code).with_for_update().first()
            if coupon and coupon.active:
                total_used = db.query(CouponUsage).filter(CouponUsage.coupon_id == coupon.id).count()
                user_used = db.query(CouponUsage).filter(
                    CouponUsage.coupon_id == coupon.id,
                    CouponUsage.user_id == message.from_user.id
                ).count()
                limits_ok = True
                if coupon.expiry_date and coupon.expiry_date < datetime.now(timezone.utc):
                    limits_ok = False
                if coupon.max_uses_total > 0 and total_used >= coupon.max_uses_total:
                    limits_ok = False
                if coupon.max_uses_per_user > 0 and user_used >= coupon.max_uses_per_user:
                    limits_ok = False
                if limits_ok:
                    usage = CouponUsage(coupon_id=coupon.id, user_id=message.from_user.id, invoice_id=invoice_id)
                    db.add(usage)
                else:
                    # Coupon no longer valid — strip it from invoice, flag for review
                    invoice.coupon_code = None
                    invoice.status = "NEEDS_REVIEW"
            db.commit()

    admin_kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="✅ تایید", callback_data=f"approve_{invoice_id}", style="success"),
        InlineKeyboardButton(text="⛔ رد کردن", callback_data=f"reject_{invoice_id}", style="danger")
    ]])
    
    # Get buyer's Telegram name and username
    buyer_name = message.from_user.full_name or message.from_user.first_name or str(message.from_user.id)
    buyer_username = f"@{message.from_user.username}" if message.from_user.username else "ندارد"
    
    # Get plan name from DB
    plan_name = ""
    if data.get('plan_id'):
        with SessionLocal() as db:
            plan = db.query(Plan).filter(Plan.id == data['plan_id']).first()
            if plan:
                plan_name = plan.name
    
    caption = f"🧾 <b>فاکتور جدید #{invoice_id}</b>\n━━━━━━━━━━━━━━━━━━\n👤 خریدار: <b>{buyer_name}</b>\n📛 یوزرنیم: <code>{buyer_username}</code>\n🆔 شناسه: <code>{message.from_user.id}</code>"
    if client_name:
        caption += f"\n🔖 نام سرویس: <code>{client_name}</code>"
    if plan_name:
        caption += f"\n📦 پلن: <b>{plan_name}</b>"
    caption += f"\n⚡ نوع عملیات: <b>{action_type}</b>"
    if is_amnezia and data.get('amnezia_server_name'):
        caption += f"\n🖥 سرور Amnezia: <b>{data['amnezia_server_name']}</b>"
    if action_type == "TOPUP":
        caption += f"\n➕ حجم درخواستی: <b>{data['added_gb']} گیگ</b>"
    caption += f"\n💰 مبلغ نهایی: <b>{final_price:,} تومان</b>"
    if discount_amount > 0:
        caption += f"\n🎫 تخفیف: <b>{discount_amount:,} تومان</b>"
    
    for admin_id in get_admin_ids():
        try: await bot.send_photo(admin_id, photo.file_id, caption=caption, reply_markup=admin_kb, parse_mode="HTML")
        except Exception as e: logger.warning(f"Failed to notify admin {admin_id}: {e}")
    
    # Animate+delete the payment message and user's receipt
    await cleanup_prev_message(bot, state, message.chat.id)
    await delete_user_message(bot, message)
    await state.clear()
    await message.answer(
        "✅ <b>رسید شما با موفقیت ثبت شد!</b> 🙏\n\n"
        "📋 <b>مراحل بعدی:</b>\n"
        "1️⃣ مدیران سیستم رسید شما را بررسی می‌کنند\n"
        "2️⃣ پس از تایید، سرویس شما آماده خواهد شد\n"
        "3️⃣ از طریق <b>📦 سرویس‌های من</b> می‌توانید وضعیت را پیگیری کنید\n\n"
        "⏳ <i>لطفاً شکیبا باشید — حداکثر تا چند ساعت آینده نتیجه اعلام می‌شود.</i>\n"
        "🌟 از اعتماد شما سپاسگزاریم!",
        reply_markup=get_main_menu(message.from_user.id), parse_mode="HTML"
    )

@dp.message(BuyFlow.wait_for_receipt)
async def receipt_invalid_input(message: types.Message, state: FSMContext):
    """Catch-all for non-photo messages during receipt state."""
    await message.answer(
        "⚠️ <b>رسید باید به صورت عکس ارسال شود.</b>\n\n"
        "لطفاً اسکرین‌شات یا عکس فیش واریزی را به عنوان <b>عکس</b> ارسال کنید.\n"
        "برای انصراف، دکمه ❌ انصراف را بزنید.",
        reply_markup=get_cancel_kb(),
        parse_mode="HTML"
    )

# ==============================================================================
# RENEWAL & TOP-UP FLOW
# ==============================================================================
@dp.callback_query(F.data.startswith("renew_"))
async def renew_plan_start(callback: types.CallbackQuery, state: FSMContext):
    email = callback.data.split("_", 1)[1]
    await state.update_data(action_type="RENEW", target_email=email)
    with SessionLocal() as db:
        plans = db.query(Plan).filter(Plan.is_active == True).all()
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"🔄 {p.name} - {p.price:,} تومان", callback_data=f"rplan_{p.id}")] for p in plans
    ] + [[InlineKeyboardButton(text="❌ انصراف", callback_data="cancel")]])
    await callback.message.edit_text(f"🔄 <b>تمدید سرویس:</b> <code>{email}</code>\n\nبرای تمدید یک پلن انتخاب کنید. حجم و روزهای باقیمانده به پلن جدید اضافه خواهد شد!", reply_markup=kb, parse_mode="HTML")

@dp.callback_query(F.data.startswith("rplan_"))
async def execute_renew(callback: types.CallbackQuery, state: FSMContext):
    plan_id = int(callback.data.split("_")[1])
    await state.update_data(plan_id=plan_id)
    await callback.message.edit_text(
        "🎫 <b>کد تخفیف دارید؟</b>\nاگر کد دارید، آن را وارد کنید. در غیر این صورت «رد شدن» را بزنید.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⏭ رد شدن", callback_data="skip_coupon")]
        ]), parse_mode="HTML"
    )
    await state.set_state(BuyFlow.wait_for_coupon)

@dp.callback_query(F.data.startswith("topup_"))
async def topup_plan_start(callback: types.CallbackQuery, state: FSMContext):
    email = callback.data.split("_", 1)[1]
    
    # Find the plan this service belongs to.
    # NEW services are provisioned on the panel as "{client_name}_{invoice_id}",
    # but their originating invoice stores only the bare client_name. RENEW/TOPUP
    # invoices, by contrast, store the full panel email. Look up both ways so any
    # purchased service can be topped up.
    with SessionLocal() as db:
        # 1) Direct match: RENEW/TOPUP invoices (stored with the full panel email) —
        #    newest plan-bearing one wins.
        invoice = db.query(Invoice).filter(
            Invoice.client_name == email,
            Invoice.status == "COMPLETE",
            Invoice.plan_id.isnot(None)
        ).order_by(Invoice.created_at.desc()).first()

        # 2) Fall back to the originating NEW invoice, whose id is the email suffix.
        if not invoice and '_' in email:
            base, _, suffix = email.rpartition('_')
            if suffix.isdigit():
                invoice = db.query(Invoice).filter(
                    Invoice.id == int(suffix),
                    Invoice.client_name == base,
                    Invoice.status == "COMPLETE",
                    Invoice.plan_id.isnot(None)
                ).first()

        if not invoice or not invoice.plan_id:
            await callback.answer(
                "⚠️ این سرویس دارای پلن مشخصی نیست. امکان خرید حجم اضافه وجود ندارد.",
                show_alert=True
            )
            return
        
        plan = db.query(Plan).filter(Plan.id == invoice.plan_id).first()
        if not plan:
            await callback.answer(
                "⚠️ پلن مربوط به این سرویس یافت نشد. امکان خرید حجم اضافه وجود ندارد.",
                show_alert=True
            )
            return
        
        price_per_gb = plan.price / plan.traffic_gb if plan.traffic_gb > 0 else 0
        if price_per_gb <= 0:
            await callback.answer(
                "⚠️ قیمت هر گیگابایت برای این پلن معتبر نیست.",
                show_alert=True
            )
            return
        
        # Get discount percent from settings
        discount_setting = db.query(AppSetting).filter(AppSetting.key == 'discount_percent').first()
        discount_pct = int(discount_setting.value) if discount_setting else 5
        
        await state.update_data(
            action_type="TOPUP",
            target_email=email,
            price_per_gb=price_per_gb,
            plan_id=plan.id,
            plan_name=plan.name,
            discount_pct=discount_pct
        )
    
    xui = XUIClient()
    full_client = await xui.get_client_full(email)
    client_data = full_client.get('client', {})
    expiry_time = client_data.get('expiryTime', 0)
    now_ms = int(time.time() * 1000)
    if expiry_time > 0 and (expiry_time - now_ms) < (5 * 86400 * 1000):
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⚠️ بله، مایل به ادامه هستم", callback_data="topup_continue")],
            [InlineKeyboardButton(text="❌ انصراف و تمدید سرویس", callback_data="cancel")]
        ])
        warning_msg = (
            "⚠️ <b>هشدار: زمان انقضا نزدیک است!</b>\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"📅 کمتر از <b>۵ روز</b> به پایان اعتبار زمانی سرویس شما باقی مانده است.\n\n"
            f"خرید حجم <u>فقط ترافیک</u> شما را افزایش می‌دهد و <b>زمان را تمدید نمی‌کند</b>.\n"
            f"در صورت فرا رسیدن تاریخ انقضا، تمامی حجم‌های باقیمانده از بین می‌روند.\n\n"
            f"💡 <b>پیشنهاد:</b> ابتدا سرویس خود را <b>تمدید</b> کنید، سپس در صورت نیاز حجم اضافه بخرید.\n\n"
            f"آیا همچنان مایل به خرید حجم اضافه هستید؟"
        )
        await callback.message.edit_text(warning_msg, reply_markup=kb, parse_mode="HTML")
    else:
        await prompt_custom_gb(callback.message, str(price_per_gb), state)

@dp.callback_query(F.data == "topup_continue")
async def topup_warning_continue(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    await prompt_custom_gb(callback.message, str(data['price_per_gb']), state)

async def prompt_custom_gb(message: types.Message, price_str: str, state: FSMContext):
    data = await state.get_data()
    plan_name = data.get('plan_name')
    discount_pct = data.get('discount_pct')
    msg = (
        f"➕ <b>مرحله 1 از 3: خرید حجم اضافه</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
    )
    if plan_name:
        msg += f"📦 پلن: <b>{plan_name}</b>\n"
    msg += f"💵 <b>قیمت هر گیگابایت:</b> {int(float(price_str)):,} تومان\n"
    if discount_pct:
        msg += f"💡 تخفیف خودکار {discount_pct}% در مرحله پرداخت اعمال می‌شود.\n"
    msg += f"\n📝 تعداد گیگابایت مورد نیاز خود را وارد کنید:\n"
    msg += f"(فقط عدد، مثال: <b>10</b>)\n"
    msg += f"<i>حداقل خرید: ۱ گیگابایت</i>"
    if hasattr(message, 'edit_text'):
        await message.edit_text(msg, parse_mode="HTML", reply_markup=get_cancel_kb())
    else:
        await message.answer(msg, parse_mode="HTML", reply_markup=get_cancel_kb())
    await state.set_state(TopupFlow.wait_for_gb)

@dp.message(TopupFlow.wait_for_gb)
async def topup_calculate_price(message: types.Message, state: FSMContext):
    if not message.text.isdigit() or int(message.text) <= 0:
        return await message.answer("⚠️ لطفاً یک عدد معتبر بزرگتر از 0 وارد کنید.", reply_markup=get_cancel_kb())
    requested_gb = int(message.text)
    data = await state.get_data()
    total_price = requested_gb * data['price_per_gb']
    await state.update_data(added_gb=requested_gb, total_price=total_price)
    await delete_user_message(bot, message)
    sent = await message.answer(
        "🎫 <b>مرحله 2 از 3: کد تخفیف</b>\n\n"
        "اگر کد تخفیف دارید، آن را وارد کنید. در غیر این صورت «رد شدن» را بزنید.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⏭ رد شدن", callback_data="skip_coupon")]
        ]), parse_mode="HTML"
    )
    await state.update_data(last_bot_msg_id=sent.message_id)
    await state.set_state(TopupFlow.wait_for_coupon)

@dp.callback_query(F.data == "skip_coupon", TopupFlow.wait_for_coupon)
async def skip_coupon_topup(callback: types.CallbackQuery, state: FSMContext):
    await state.update_data(coupon_code=None)
    await cleanup_prev_message(bot, state, callback.message.chat.id)
    await show_payment(callback.message, state)

@dp.message(TopupFlow.wait_for_coupon)
async def process_coupon_topup(message: types.Message, state: FSMContext):
    code = message.text.strip()
    with SessionLocal() as db:
        coupon = db.query(Coupon).filter(Coupon.code == code, Coupon.active == True).first()
        if not coupon:
            await message.answer("❌ کد تخفیف معتبر نیست. لطفاً مجدداً وارد کنید یا رد شوید.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⏭ رد شدن", callback_data="skip_coupon")]]))
            return
        if coupon.expiry_date and coupon.expiry_date < datetime.now(timezone.utc):
            await message.answer("❌ این کد تخفیف منقضی شده است.")
            return
        total_used = db.query(CouponUsage).filter(CouponUsage.coupon_id == coupon.id).count()
        if coupon.max_uses_total > 0 and total_used >= coupon.max_uses_total:
            await message.answer("❌ این کد تخفیف دیگر قابل استفاده نیست.")
            return
        user_used = db.query(CouponUsage).filter(CouponUsage.coupon_id == coupon.id, CouponUsage.user_id == message.from_user.id).count()
        if coupon.max_uses_per_user > 0 and user_used >= coupon.max_uses_per_user:
            await message.answer("❌ شما قبلاً از این کد تخفیف استفاده کرده‌اید.")
            return
        applicable = coupon.applicable_to
        if applicable != 'all':
            allowed = applicable.split(',')
            if 'topup' not in allowed:
                await message.answer("❌ این کد تخفیف برای خرید حجم اضافه قابل استفاده نیست.")
                return
        await state.update_data(coupon_code=code)
    await delete_user_message(bot, message)
    await cleanup_prev_message(bot, state, message.chat.id)
    await show_payment(message, state)

# ==============================================================================
# AMNEZIA SERVICE FLOW (experimental; visibility gated by amnezia_visible)
# Callback prefixes used here all start with "amz" so they never collide with
# the XUI handlers above ("renew_", "topup_", "stat_", ...).
# ==============================================================================
def _amz_time_left(exp_dt):
    """Human-readable time remaining until an aware datetime."""
    if exp_dt is None:
        return "♾️ نامحدود"
    delta = exp_dt - datetime.now(timezone.utc)
    if delta.total_seconds() <= 0:
        return "منقضی شده"
    if delta.days > 0:
        return f"{delta.days} روز و {delta.seconds // 3600} ساعت"
    return f"{delta.seconds // 3600} ساعت و {(delta.seconds % 3600) // 60} دقیقه"

async def _amz_server_label(amz: AmneziaClient, server_id: int) -> str:
    """Best-effort human label for a server id (uses resolved names)."""
    try:
        servers = await amz.list_servers_detailed()
        for s in servers:
            if s['id'] == server_id:
                return s.get('name') or f"سرور {server_id}"
    except Exception:
        pass
    return f"سرور {server_id}"

AMNEZIA_ONE_DEVICE_WARNING = (
    "⚠️ <b>نکته مهم درباره سرویس Amnezia</b>\n"
    "━━━━━━━━━━━━━━━━━━\n\n"
    "▫️ هر کانکشن Amnezia همزمان تنها روی <b>یک دستگاه</b> قابل استفاده است.\n"
    "▫️ اتصال یک دستگاه دوم، اتصال دستگاه اول را قطع می‌کند.\n"
    "▫️ این محدودیت ذاتی پروتکل است و <b>قابل حذف نمی‌باشد</b>.\n\n"
    "با ادامه خرید، تأیید می‌کنید که از این موضوع اطلاع دارید."
)

@dp.callback_query(F.data.startswith("amzplan_"))
async def amz_plan_selected(callback: types.CallbackQuery, state: FSMContext):
    plan_id = int(callback.data.split("_")[1])
    if not amnezia_visible(callback.from_user.id):
        return await callback.answer("⛔ این بخش در دسترس شما نیست.", show_alert=True)
    with SessionLocal() as db:
        plan = db.query(Plan).filter(Plan.id == plan_id, Plan.is_active == True).first()
        owned_before = db.query(AmneziaService.id).filter(
            AmneziaService.telegram_user_id == callback.from_user.id).first() is not None
    if not plan or plan.service_type != 'amnezia':
        return await callback.answer("⚠️ این پلن Amnezia معتبر نیست.", show_alert=True)
    await state.update_data(plan_id=plan_id, action_type="NEW")
    if not owned_before:
        return await callback.message.edit_text(
            AMNEZIA_ONE_DEVICE_WARNING,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="✅ فهمیدم، ادامه خرید", callback_data=f"amzok_{plan_id}")],
                [InlineKeyboardButton(text="❌ انصراف", callback_data="cancel")],
            ]), parse_mode="HTML")
    await _amz_show_server_picker(callback.message, state, plan)

@dp.callback_query(F.data.startswith("amzok_"))
async def amz_ack_one_device(callback: types.CallbackQuery, state: FSMContext):
    plan_id = int(callback.data.split("_")[1])
    if not amnezia_visible(callback.from_user.id):
        return await callback.answer("⛔ این بخش در دسترس شما نیست.", show_alert=True)
    with SessionLocal() as db:
        plan = db.query(Plan).filter(Plan.id == plan_id, Plan.is_active == True).first()
    if not plan or plan.service_type != 'amnezia':
        return await callback.answer("⚠️ این پلن Amnezia معتبر نیست.", show_alert=True)
    await _amz_show_server_picker(callback.message, state, plan)

async def _amz_show_server_picker(message: types.Message, state: FSMContext, plan: Plan):
    """Show all discovered Amnezia servers (real names when known) for plan selection."""
    unlimited = (plan.traffic_gb or 0) == 0
    amz = AmneziaClient()
    try:
        try:
            servers = await amz.list_servers_detailed()
        except AttributeError:
            servers = await amz.list_servers()
    except AmneziaError as e:
        logger.error(f"Amnezia server listing failed: {e}")
        return await message.answer("❌ خطا در ارتباط با پنل Amnezia.")
    finally:
        await amz.close()
    if not servers:
        return await message.answer("⚠️ هیچ سروری در پنل Amnezia یافت نشد.")
    kb_rows = []
    for s in servers:
        sname = s.get('name') or f"سرور {s['id']}"
        if not s.get('alive', True):
            sname += " (قطع)"
        kb_rows.append([InlineKeyboardButton(text=f"🖥 {sname}",
                                             callback_data=f"amzsrv_{plan.id}_{s['id']}")])
    kb_rows.append([InlineKeyboardButton(text="❌ انصراف", callback_data="cancel")])
    text = (
        f"🟣 <b>پلن:</b> {plan.name}\n\n"
        f"🖥 <b>انتخاب لوکیشن سرور</b>\n\n"
        f"سرور مورد نظر خود را انتخاب کنید:"
    )
    if unlimited:
        text += ("\n\n⚠️ <b>توجه:</b> این پلن نامحدود فقط روی <b>همان یک سروری</b> که "
                 "اینجا انتخاب می‌کنید فعال خواهد شد؛ بعداً قابل تغییر نیست.")
    try:
        await message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_rows),
                                parse_mode="HTML")
    except TelegramBadRequest:
        await message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_rows),
                             parse_mode="HTML")

@dp.callback_query(F.data.startswith("amzsrv_"))
async def amz_server_selected(callback: types.CallbackQuery, state: FSMContext):
    _, plan_id, server_id = callback.data.split("_")
    plan_id, server_id = int(plan_id), int(server_id)
    # Resolve the human label for the receipt/invoice from the same listing.
    label = f"سرور {server_id}"
    amz = AmneziaClient()
    try:
        for s in await amz.list_servers_detailed():
            if s['id'] == server_id and s.get('name'):
                label = s['name']
                break
    except Exception:
        pass
    finally:
        await amz.close()
    await state.update_data(plan_id=plan_id, amnezia_server_id=server_id,
                            amnezia_server_name=label,
                            amz_username=None, amz_password=None)
    await callback.message.edit_text(
        "🔐 <b>حساب کاربری پنل Amnezia (اختیاری)</b>\n\n"
        "می‌توانید برای ورود به پنل وب Amnezia نام کاربری و رمز عبور دلخواه انتخاب کنید:\n\n"
        "▫️ فرمت پیام: <code>username password</code>\n"
        "▫️ اگر فقط نام کاربری بفرستید، رمز به صورت خودکار ساخته می‌شود\n"
        "▫️ نام کاربری: حروف انگلیسی/عدد/زیرخط (۳ تا ۳۲ کاراکتر)\n"
        "▫️ رمز عبور: حداقل ۶ کاراکتر\n\n"
        "💡 اگر این مرحله را رد کنید، حساب به صورت خودکار ساخته می‌شود.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⏭ رد شدن و ساخت خودکار", callback_data="amzskipcreds")]
        ]), parse_mode="HTML"
    )
    await state.set_state(BuyFlow.wait_for_amz_account)

@dp.callback_query(BuyFlow.wait_for_amz_account, F.data == "amzskipcreds")
async def amz_skip_creds(callback: types.CallbackQuery, state: FSMContext):
    await state.update_data(amz_username=None, amz_password=None)
    await cleanup_prev_message(bot, state, callback.message.chat.id)
    await _amz_send_coupon_step(callback.message, state)

@dp.message(BuyFlow.wait_for_amz_account)
async def amz_process_creds(message: types.Message, state: FSMContext):
    text = (message.text or "").strip()
    parts = text.split()
    if not (1 <= len(parts) <= 2) or not text:
        return await message.answer(
            "⚠️ <b>فرمت نامعتبر.</b>\n\n"
            "پیام را به شکل <code>username password</code> بفرستید "
            "(یا فقط <code>username</code>)، یا «رد شدن» را بزنید.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⏭ رد شدن و ساخت خودکار", callback_data="amzskipcreds")]
            ]), parse_mode="HTML")
    username = parts[0]
    if not re.match(r"^[A-Za-z0-9_]{3,32}$", username):
        return await message.answer(
            "⚠️ <b>نام کاربری نامعتبر.</b>\n"
            "فقط حروف انگلیسی، عدد و زیرخط؛ بین ۳ تا ۳۲ کاراکتر. دوباره بفرستید:",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⏭ رد شدن و ساخت خودکار", callback_data="amzskipcreds")]
            ]), parse_mode="HTML")
    if len(parts) == 2 and len(parts[1]) < 6:
        return await message.answer(
            "⚠️ <b>رمز عبور کوتاه است.</b>\nحداقل ۶ کاراکتر. دوباره بفرستید:",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⏭ رد شدن و ساخت خودکار", callback_data="amzskipcreds")]
            ]), parse_mode="HTML")
    # Uniqueness is enforced again at provisioning time (panel-side check).
    amz = AmneziaClient()
    try:
        available = await amz.is_username_available(username, message.from_user.id)
    except Exception:
        available = True  # don't block the purchase on a transient panel error
    finally:
        await amz.close()
    if not available:
        return await message.answer(
            "❌ <b>این نام کاربری قبلاً در پنل گرفته شده است.</b>\n"
            "لطفاً نام کاربری دیگری بفرستید (یا «رد شدن» را بزنید):",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⏭ رد شدن و ساخت خودکار", callback_data="amzskipcreds")]
            ]), parse_mode="HTML")
    import secrets as _secrets
    password = parts[1] if len(parts) == 2 else _secrets.token_urlsafe(12)
    await state.update_data(amz_username=username, amz_password=password)
    await delete_user_message(bot, message)
    await cleanup_prev_message(bot, state, message.chat.id)
    await _amz_send_coupon_step(message, state)

async def _amz_send_coupon_step(message: types.Message, state: FSMContext):
    # Choke point of the Amnezia NEW flow (skip-creds and custom-creds both
    # land here): stamp the invoice action so process_receipt knows this is
    # an Amnezia purchase (auto service name, description JSON, dispatch).
    await state.update_data(action_type="AMNEZIA_NEW")
    sent = await message.answer(
        "🎫 <b>کد تخفیف</b>\n\n"
        "اگر کد تخفیف دارید، آن را وارد کنید. در غیر این صورت «رد شدن» را بزنید.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⏭ رد شدن", callback_data="skip_coupon")]
        ]), parse_mode="HTML")
    await state.update_data(last_bot_msg_id=sent.message_id)
    await state.set_state(BuyFlow.wait_for_coupon)

@dp.callback_query(F.data.startswith("amzren_"))
async def amz_renew_start(callback: types.CallbackQuery, state: FSMContext):
    sid = int(callback.data.split("_")[1])
    with SessionLocal() as db:
        svc = db.query(AmneziaService).filter(AmneziaService.id == sid).first()
    if not svc:
        return await callback.answer("⚠️ سرویس یافت نشد.", show_alert=True)
    if svc.telegram_user_id != callback.from_user.id:
        return await callback.answer("⛔ فقط مالک سرویس می‌تواند تمدید کند.", show_alert=True)
    await state.update_data(action_type="AMNEZIA_RENEW", target_service_id=sid)
    with SessionLocal() as db:
        plans = db.query(Plan).filter(Plan.is_active == True, Plan.service_type == 'amnezia').all()
    if not plans:
        return await callback.answer("⚠️ هیچ پلن Amnezia فعالی برای تمدید وجود ندارد.", show_alert=True)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"🔄 {p.name} - {p.price:,} تومان", callback_data=f"amzp_{p.id}_{sid}")] for p in plans
    ] + [[InlineKeyboardButton(text="❌ انصراف", callback_data="cancel")]])
    await callback.message.edit_text(
        f"🔄 <b>تمدید سرویس:</b> <code>{svc.name}</code>\n\nبرای تمدید یک پلن انتخاب کنید. حجم و روزهای باقیمانده حفظ و پلن جدید اضافه خواهد شد!",
        reply_markup=kb, parse_mode="HTML")

@dp.callback_query(F.data.startswith("amzp_"))
async def amz_renew_plan_picked(callback: types.CallbackQuery, state: FSMContext):
    _, plan_id, sid = callback.data.split("_")
    await state.update_data(plan_id=int(plan_id), target_service_id=int(sid))
    await callback.message.edit_text(
        "🎫 <b>کد تخفیف دارید؟</b>\nاگر کد دارید، آن را وارد کنید. در غیر این صورت «رد شدن» را بزنید.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⏭ رد شدن", callback_data="skip_coupon")]
        ]), parse_mode="HTML"
    )
    await state.set_state(BuyFlow.wait_for_coupon)

@dp.callback_query(F.data.startswith("amztop_"))
async def amz_topup_start(callback: types.CallbackQuery, state: FSMContext):
    sid = int(callback.data.split("_")[1])
    with SessionLocal() as db:
        svc = db.query(AmneziaService).filter(AmneziaService.id == sid).first()
        if not svc:
            return await callback.answer("⚠️ سرویس یافت نشد.", show_alert=True)
        if svc.telegram_user_id != callback.from_user.id:
            return await callback.answer("⛔ فقط مالک سرویس می‌تواند حجم اضافه بخرد.", show_alert=True)
        if (svc.quota_bytes or 0) == 0:
            # Unlimited plan: the panel quota is account-wide; adding GB would
            # silently CAP the account to the purchased amount.
            return await callback.answer(
                "♾️ این سرویس نامحدود است و خرید حجم اضافه ندارد.",
                show_alert=True)
        # Price per GB comes from the most recent plan-bearing COMPLETE invoice
        inv = db.query(Invoice).filter(
            Invoice.amnezia_service_id == sid,
            Invoice.status == "COMPLETE",
            Invoice.plan_id.isnot(None)
        ).order_by(Invoice.created_at.desc()).first()
        plan = db.query(Plan).filter(Plan.id == inv.plan_id).first() if inv else None
        if not plan or plan.traffic_gb <= 0 or plan.price <= 0:
            return await callback.answer(
                "⚠️ قیمت هر گیگابایت برای این سرویس مشخص نیست. با پشتیبانی تماس بگیرید.",
                show_alert=True)
        discount_setting = db.query(AppSetting).filter(AppSetting.key == 'discount_percent').first()
        discount_pct = int(discount_setting.value) if discount_setting else 5

    await state.update_data(
        action_type="AMNEZIA_TOPUP",
        target_service_id=sid,
        price_per_gb=plan.price / plan.traffic_gb,
        plan_id=plan.id,
        plan_name=plan.name,
        discount_pct=discount_pct,
    )
    await prompt_custom_gb(callback.message, str(plan.price / plan.traffic_gb), state)

@dp.callback_query(F.data.startswith("amzcfg_"))
async def amz_get_config(callback: types.CallbackQuery):
    sid = int(callback.data.split("_")[1])
    with SessionLocal() as db:
        svc = db.query(AmneziaService).filter(AmneziaService.id == sid).first()
    if not svc:
        return await callback.answer("⚠️ سرویس یافت نشد.", show_alert=True)
    if svc.telegram_user_id != callback.from_user.id and callback.from_user.id not in get_admin_ids():
        return await callback.answer("⛔ دسترسی ندارید.", show_alert=True)
    await callback.answer("⏳ در حال دریافت کانفیگ...")
    await bot.send_chat_action(callback.message.chat.id, "typing")  # panel can be slow

    server_id, client_id = svc.server_id, svc.client_id
    amz = AmneziaClient()
    try:
        # Resolve against the panel's LIVE connection record first — after a
        # server reorder the stored index may be stale until the hourly sweep
        # re-anchors it. The record is authoritative for both fields.
        with SessionLocal() as db:
            mapping_id = db.query(AmneziaService).filter(
                AmneziaService.id == sid).first().panel_user_id
        if mapping_id:
            try:
                for c in await amz.get_user_connections(mapping_id):
                    if c.get('id') == svc.connection_id:
                        server_id = c.get('server_id', server_id)
                        client_id = c.get('client_id', client_id)
                        break
            except AmneziaError as e:
                logger.warning(f"amz_get_config: live resolve failed, using stored ids: {e}")
        cfg = await amz.get_connection_config(server_id, client_id)
    except AmneziaError as e:
        logger.error(f"amz_get_config failed for service {sid}: {e}")
        return await callback.message.answer("❌ خطا در دریافت کانفیگ از پنل Amnezia. بعداً دوباره تلاش کنید.")
    finally:
        await amz.close()
    vpn_link = cfg.get('vpn_link')
    config_text = cfg.get('config') or ''
    if vpn_link:
        await callback.message.answer(
            f"🔗 <b>لینک اتصال Amnezia:</b>\n<code>{vpn_link}</code>\n\n"
            f"👆 این لینک را کپی و در برنامه Amnezia وارد کنید (گزینه Import).",
            parse_mode="HTML")
    if config_text:
        await callback.message.answer_document(
            BufferedInputFile(config_text.encode('utf-8'), filename=f"{svc.name}.conf"),
            caption=f"📄 فایل کانفیگ سرویس <code>{svc.name}</code>", parse_mode="HTML")

@dp.callback_query(F.data.startswith("amzstat_"))
async def amz_view_stats(callback: types.CallbackQuery):
    sid = int(callback.data.split("_")[1])
    is_admin_viewer = callback.from_user.id in get_admin_ids()
    with SessionLocal() as db:
        svc = db.query(AmneziaService).filter(AmneziaService.id == sid).first()
        if not svc:
            return await callback.answer("⚠️ سرویس یافت نشد.", show_alert=True)
        if svc.telegram_user_id != callback.from_user.id and not is_admin_viewer:
            return await callback.answer("⛔ دسترسی ندارید.", show_alert=True)
        owner_id = svc.telegram_user_id
        svc_info = {"name": svc.name, "status": svc.status, "quota": svc.quota_bytes,
                    "expiry": svc.expiry_date, "server_id": svc.server_id,
                    "panel_username": svc.panel_username}
    await bot.send_chat_action(callback.message.chat.id, "typing")  # stats fetch hits the panel
    amz = AmneziaClient()
    try:
        stats = None
        server_label = await _amz_server_label(amz, svc_info["server_id"])
        if svc_info.get("panel_username"):
            try:
                stats = await amz.get_user_stats(svc_info["panel_username"])
            except AmneziaError as e:
                logger.warning(f"amz stats fetch failed for {svc_info['panel_username']}: {e}")
    finally:
        await amz.close()

    exp_dt = (stats or {}).get('expiration_date') or svc_info["expiry"]
    expired = exp_dt is not None and exp_dt <= datetime.now(timezone.utc)
    enabled = (stats or {}).get('enabled', True)
    if expired:
        status_badge = "🔴 منقضی شده"
    elif not enabled:
        status_badge = "🔴 غیرفعال"
    elif svc_info["status"] != 'active':
        status_badge = "⚪ " + svc_info["status"]
    else:
        status_badge = "🟢 فعال"

    used_bytes = (stats or {}).get('used', 0)
    limit_bytes = (stats or {}).get('limit') or svc_info["quota"] or 0
    progress_bar = get_progress_bar(used_bytes, limit_bytes) if limit_bytes > 0 else "♾️"
    text = (
        f"📊 <b>داشبورد سرویس Amnezia</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"🔖 <b>نام:</b> <code>{svc_info['name']}</code>\n"
        f"🖥 <b>سرور:</b> {server_label}\n"
        f"📌 <b>وضعیت:</b> {status_badge}\n"
        f"\n"
        f"📈 <b>مصرف داده:</b>\n"
        f"{progress_bar}\n"
        f"📥 <b>مصرف شده:</b> {format_size(used_bytes)}\n"
        f"📦 <b>کل حجم:</b> {format_size(limit_bytes) if limit_bytes > 0 else '♾️ نامحدود'}\n"
        f"\n"
        f"⏳ <b>زمان باقیمانده:</b> {_amz_time_left(exp_dt)}\n"
        f"📅 <b>تاریخ انقضا:</b> {exp_dt.strftime('%Y-%m-%d %H:%M') if exp_dt else '♾️ نامحدود'}\n"
        f"━━━━━━━━━━━━━━━━━━"
    )
    owner_only = owner_id == callback.from_user.id
    kb_buttons = [[InlineKeyboardButton(text="📄 دریافت لینک و کانفیگ", callback_data=f"amzcfg_{sid}")]]
    if owner_only:
        kb_buttons.append([InlineKeyboardButton(text="🔑 مشخصات حساب پنل", callback_data=f"amzcreds_{sid}")])
    if owner_only and svc_info["status"] == 'active':
        action_row = [InlineKeyboardButton(text="🔄 تمدید سرویس", callback_data=f"amzren_{sid}")]
        if (svc_info["quota"] or 0) != 0:   # unlimited services can't be topped up
            action_row.append(InlineKeyboardButton(text="➕ خرید حجم اضافه", callback_data=f"amztop_{sid}"))
        kb_buttons.append(action_row)
    back_cb = "admin_amnezia" if (is_admin_viewer and not owner_only) else "my_plans"
    # Official AmneziaVPN apps
    kb_buttons.append([InlineKeyboardButton(text="❓ راهنما و رفع اشکال", callback_data="amzfaq")])
    kb_buttons.append([InlineKeyboardButton(text="📱 دانلود برنامه — Android", url=PLAY_STORE_URL),
                       InlineKeyboardButton(text="iOS", url=APP_STORE_URL)])
    kb_buttons.append([InlineKeyboardButton(text="⬅️ بازگشت", callback_data=back_cb)])
    await callback.message.edit_text(text, parse_mode="HTML",
                                     reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_buttons))

@dp.callback_query(F.data.startswith("amzcreds_"))
async def amz_show_account_creds(callback: types.CallbackQuery):
    """Show the saved Amnezia panel-account credentials (owner only)."""
    sid = int(callback.data.split("_")[1])
    with SessionLocal() as db:
        svc = db.query(AmneziaService).filter(AmneziaService.id == sid).first()
        if not svc:
            return await callback.answer("⚠️ سرویس یافت نشد.", show_alert=True)
        if svc.telegram_user_id != callback.from_user.id:
            return await callback.answer("⛔ فقط مالک سرویس می‌تواند مشخصات حساب را ببیند.", show_alert=True)
    pw_line = (f"🔑 <b>رمز عبور:</b> <code>{svc.panel_password}</code>"
               if svc.panel_password
               else "🔑 <b>رمز عبور:</b> <i>موجود نیست.</i>")
    panel_url = os.getenv('AMNEZIA_API_URL', '').rstrip('/')
    await callback.message.answer(
        f"🔐 <b>مشخصات حساب Amnezia این سرویس</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"🔖 سرویس: <code>{svc.name}</code>\n"
        f"👤 <b>نام کاربری:</b> <code>{svc.panel_username or '—'}</code>\n"
        f"{pw_line}\n\n"
        f"🌐 ورود به پنل وب: {panel_url}\n\n"
        f"💡 با این مشخصات می‌توانید در پنل وب وارد شوید و کانفیگ‌های خود را ببینید.",
        parse_mode="HTML")

# ========== Amnezia FAQ (end-user self-service) ==========
# Ordered (title, html_answer) pairs — indices are baked into callback data
# (amzfaq_<i>), so APPEND-ONLY: reordering breaks buttons already on screen.
AMNEZIA_FAQ = [
    ("🔌 کانفیگ وصل نمی‌شود",
     "این مراحل را به ترتیب امتحان کنید:\n\n"
     "1️⃣ اینترنت دستگاه را بررسی کنید (یک سایت را بدون فیلترشکن باز کنید)\n"
     "2️⃣ در برنامه Amnezia دکمه قطع اتصال را بزنید و دوباره وصل شوید\n"
     "3️⃣ حالت هواپیما را یک‌بار روشن و خاموش کنید\n"
     "4️⃣ برنامه Amnezia را کامل ببندید و دوباره باز کنید\n\n"
     "💡 اگر با Wi-Fi مشکل دارید، با اینترنت همراه امتحان کنید (و برعکس).\n\n"
     "❓ حل نشد؟ از دکمه «💬 تماس با پشتیبانی» زیر همین صفحه کمک بگیرید."),
    ("🔄 قبلاً وصل می‌شد، الان نمی‌شود",
     "دلایل رایج به ترتیب اهمیت:\n\n"
     "1️⃣ <b>انقضای سرویس</b> — از «📦 سرویس‌های من» سرویس خود را باز کنید و تاریخ انقضا را ببینید؛ اگر منقضی شده، با دکمه 🔄 تمدید فعالش کنید\n"
     "2️⃣ برنامه Amnezia را کامل ببندید و دوباره باز کنید\n"
     "3️⃣ گوشی را ری‌استارت کنید\n\n"
     "💡 بعد از تمدید هم اگر وصل نشد، یک‌بار حالت هواپیما را روشن و خاموش کنید."),
    ("📶 سرعت کم است",
     "برای بهبود سرعت:\n\n"
     "1️⃣ سرعت را بدون فیلترشکن تست کنید تا مطمئن شوید مشکل از اینترنت پایه نیست\n"
     "2️⃣ در برنامه Amnezia قطع و دوباره وصل شوید\n"
     "3️⃣ مودم/روتر را ری‌استارت کنید\n"
     "4️⃣ در ساعات شلوغ شبانه افت سرعت طبیعی است\n\n"
     "💬 اگر سرعت به‌صورت مداوم پایین است، از دکمه پشتیبانی گزارش دهید تا بررسی شود."),
    ("📥 چطور لینک vpn:// را وارد کنم؟",
     "مرحله به مرحله:\n\n"
     "1️⃣ لینک <code>vpn://...</code> را از ربات لمس کنید تا کپی شود\n"
     "2️⃣ برنامه Amnezia را باز کنید\n"
     "3️⃣ روی ➕ (یا Import / افزودن کانفیگ) بزنید\n"
     "4️⃣ گزینه «Paste from clipboard» یا «ورود داده» را انتخاب کنید\n"
     "5️⃣ کانفیگ ساخته می‌شود — دکمه اتصال را بزنید ✅"),
    ("📄 چطور از فایل کانفیگ استفاده کنم؟",
     "1️⃣ فایل <code>.conf</code> را از ربات دانلود کنید\n"
     "2️⃣ در برنامه Amnezia روی ➕ بزنید\n"
     "3️⃣ گزینه «Import from file» / انتخاب فایل را بزنید\n"
     "4️⃣ فایل دانلودشده را انتخاب و ذخیره کنید\n"
     "5️⃣ اتصال را روشن کنید ✅"),
    ("📱 چرا فقط یک دستگاه وصل می‌شود؟",
     "هر کانکشن Amnezia همزمان فقط روی <b>یک دستگاه</b> کار می‌کند:\n\n"
     "▫️ وقتی دستگاه دوم وصل می‌شود، دستگاه اول قطع می‌شود\n"
     "▫️ این محدودیت ذاتی پروتکل است و <b>قابل حذف نیست</b>\n\n"
     "💡 برای چند دستگاه، برای هر دستگاه یک سرویس جداگانه تهیه کنید."),
    ("⏳ چقدر حجم و زمان باقی مانده؟",
     "از منوی اصلی «📦 سرویس‌های من» را باز کنید و روی سرویس Amnezia خود بزنید:\n\n"
     "📊 مصرف و حجم کل همان‌جا نمایش داده می‌شود\n"
     "📅 تاریخ انقضا و زمان باقیمانده هم مشخص است\n\n"
     "🔔 حدود ۳ روز قبل از انقضا، به‌صورت خودکار پیام هشدار دریافت می‌کنید."),
    ("♻️ سرویس منقضی شده چه کنم؟",
     "نگران نباشید، اطلاعات شما از بین نمی‌رود:\n\n"
     "1️⃣ وارد «📦 سرویس‌های من» شوید و سرویس خود را باز کنید\n"
     "2️⃣ دکمه 🔄 تمدید را بزنید، پلن موردنظر را انتخاب و رسید را ارسال کنید\n\n"
     "✅ پس از تایید، حجم و زمان تازه اضافه می‌شود و همان کانفیگ قبلی دوباره کار می‌کند.\n"
     "⚠️ سرویس منقضی پس از چند روز به‌صورت خودکار غیرفعال می‌شود؛ هرچه زودتر تمدید کنید بهتر است."),
]

@dp.callback_query(F.data == "amzfaq")
async def amzfaq_menu(callback: types.CallbackQuery):
    kb_rows = [[InlineKeyboardButton(text=title, callback_data=f"amzfaq_{i}")]
               for i, (title, _) in enumerate(AMNEZIA_FAQ)]
    kb_rows.append([InlineKeyboardButton(text="🏠 منوی اصلی", callback_data="main_menu")])
    await callback.message.edit_text(
        "❓ <b>راهنمای Amnezia و رفع اشکال</b>\n"
        "━━━━━━━━━━━━━━━━━━\n"
        "پاسخ پرتکرارترین سوالات — موضوع خود را انتخاب کنید:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_rows), parse_mode="HTML")

@dp.callback_query(F.data.startswith("amzfaq_"))
async def amzfaq_answer(callback: types.CallbackQuery):
    try:
        idx = int(callback.data.split("_")[1])
        title, answer = AMNEZIA_FAQ[idx]
    except (ValueError, IndexError):
        return await callback.answer("⚠️ این سوال یافت نشد.", show_alert=True)
    with SessionLocal() as db:
        s = db.query(AppSetting).filter(AppSetting.key == 'support_url').first()
    support_url = s.value if s else None
    rows = [[InlineKeyboardButton(text="❓ سوالات دیگر", callback_data="amzfaq")]]
    bottom_row = [InlineKeyboardButton(text="💬 تماس با پشتیبانی", url=support_url)] if support_url else []
    bottom_row.append(InlineKeyboardButton(text="🏠 منوی اصلی", callback_data="main_menu"))
    rows.append(bottom_row)
    await callback.message.edit_text(
        f"{title}\n━━━━━━━━━━━━━━━━━━\n\n{answer}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=rows), parse_mode="HTML")

@dp.callback_query(F.data == "admin_amnezia")
async def admin_amnezia_menu(callback: types.CallbackQuery):
    if callback.from_user.id not in get_admin_ids():
        return
    with SessionLocal() as db:
        rows = db.query(AmneziaService).order_by(AmneziaService.id.desc()).limit(30).all()
    now_utc = datetime.now(timezone.utc)
    text = (
        "🟣 <b>مدیریت سرویس‌های Amnezia</b>\n━━━━━━━━━━━━━━━━━━\n"
        f"📦 کل سرویس‌ها (۳۰ اخیر): <b>{len(rows)}</b>\n"
        f"⚙️ نمایش برای کاربران عادی: <b>{'روشن' if AMNEZIA_ENABLED else 'خاموش (فقط ادمین‌ها)'}</b>\n"
    )
    kb_buttons = []
    text += "<blockquote expandable>"
    for s in rows:
        live = s.status == 'active' and (s.expiry_date is None or s.expiry_date > now_utc)
        emoji = '🟢' if live else '🔴'
        exp_str = s.expiry_date.strftime('%Y-%m-%d') if s.expiry_date else '—'
        text += f"\n{emoji} <code>{s.name}</code> | 👤 <code>{s.telegram_user_id}</code> | 📅 {exp_str}"
        kb_buttons.append([InlineKeyboardButton(text=f"{emoji} {s.name}", callback_data=f"amzstat_{s.id}")])
    if not rows:
        text += "\n هنوز سرویسی ثبت نشده است."
    text += "</blockquote>"
    kb_buttons.append([InlineKeyboardButton(text="🔄 بروزرسانی", callback_data="admin_amnezia")])
    kb_buttons.append([InlineKeyboardButton(text="⚙️ پنل مدیریت", callback_data="admin_panel")])
    await callback.message.edit_text(text, parse_mode="HTML",
                                     reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_buttons))

# ==============================================================================
# RESELLER PANEL (allowance-funded, no payment flow)
# ==============================================================================
def _reseller_balance_text(user_id: int) -> str:
    bal = get_reseller_balance(user_id)
    if not bal:
        logger.info(f"_reseller_balance_text: no balance for user {user_id}")
        return ""
    total_available, total_used = bal   # total_available is already the remaining bytes (including unused packs)
    remaining = total_available          # already remaining
    total_capacity = total_available + total_used  # total granted capacity
    logger.info(f"_reseller_balance_text: user={user_id}, total_available={total_available}, total_used={total_used}, remaining={remaining}, total_capacity={total_capacity}")
    bar = get_progress_bar(total_used, total_capacity, 10) if total_capacity > 0 else "♾️"
    text = (
        f"📊 <b>موجودی ترافیک نمایندگی</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"{bar}\n"
        f"📥 مصرف‌شده: <b>{format_size(total_used)}</b>\n"
        f"📦 باقیمانده: <b>{format_size(remaining)}</b> از {format_size(total_capacity)}\n"
    )
    # Fetch packs for breakdown
    with SessionLocal() as db:
        packs = db.query(ResellerPack).filter(ResellerPack.reseller_id == user_id).order_by(ResellerPack.expiry_date.asc()).all()
    if packs:
        text += f"\n<b>بسته‌های ترافیک:</b>\n"
        for pack in packs:
            pack_remaining = max(0, pack.granted_bytes - pack.used_bytes)
            expiry_date = pack.expiry_date.strftime('%Y-%m-%d')
            now = datetime.now(timezone.utc)
            is_expired = pack.expiry_date < now or not pack.is_active
            status_icon = "🔴" if is_expired else ("🟢" if pack_remaining > 0 else "⚪")
            text += f"{status_icon} {format_size(pack.granted_bytes)} | مصرف: {format_size(pack.used_bytes)} | باقی: {format_size(pack_remaining)} | انقضا: {expiry_date}\n"
    else:
        text += f"\n❌ هیچ بسته‌ای وجود ندارد."
    return text

@dp.callback_query(F.data == "reseller_panel")
async def reseller_panel_cb(callback: types.CallbackQuery, state: FSMContext):
    if not is_reseller(callback.from_user.id):
        return await callback.answer("⛔ دسترسی نمایندگی ندارید.", show_alert=True)
    await state.clear()
    try: await callback.answer()
    except Exception as e: logger.warning(f"Callback answer failed: {e}")
    await callback.message.answer("🧑‍💼 <b>پنل نمایندگی</b>", reply_markup=get_reseller_menu(), parse_mode="HTML")

@dp.callback_query(F.data == "res_balance")
async def reseller_balance_cb(callback: types.CallbackQuery):
    if not is_reseller(callback.from_user.id):
        return await callback.answer("⛔ دسترسی نمایندگی ندارید.", show_alert=True)
    await callback.message.edit_text(
        _reseller_balance_text(callback.from_user.id),
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ بازگشت", callback_data="reseller_panel")]
        ]), parse_mode="HTML"
    )

@dp.callback_query(F.data == "res_list")
async def reseller_list_cb(callback: types.CallbackQuery):
    if not is_reseller(callback.from_user.id):
        return await callback.answer("⛔ دسترسی نمایندگی ندارید.", show_alert=True)
    rid = callback.from_user.id
    with SessionLocal() as db:
        invoices = db.query(Invoice).filter(
            Invoice.reseller_id == rid,
            Invoice.status == "COMPLETE",
            Invoice.client_name.isnot(None),
            Invoice.client_name != ""
        ).order_by(Invoice.id.desc()).limit(50).all()
        emails = list(dict.fromkeys([inv.client_name for inv in invoices]))
    if not emails:
        return await callback.message.edit_text(
            "📦 <b>سرویس‌های نمایندگی</b>\n━━━━━━━━━━━━━━━━━━\n⚠️ هنوز هیچ سرویسی نساخته‌اید.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="➕ ساخت سرویس جدید", callback_data="res_new")],
                [InlineKeyboardButton(text="⬅️ بازگشت", callback_data="reseller_panel")]
            ]), parse_mode="HTML"
        )
    
    # Fetch live status for each service to show active/expired badges (same as my_plans_content)
    xui = get_xui_client()
    statuses = {}
    try:
        fetch_tasks = [xui.get_client_full(email) for email in emails]
        results = await asyncio.gather(*fetch_tasks, return_exceptions=True)
        
        for email, result in zip(emails, results):
            if isinstance(result, Exception) or result is None or 'client' not in result:
                statuses[email] = 'unknown'
            else:
                client = result['client']
                expiry = client.get('expiryTime', 0)
                now_ms = int(time.time() * 1000)
                if expiry > 0 and now_ms > expiry:
                    statuses[email] = 'expired'
                elif not client.get('enable', True):
                    statuses[email] = 'disabled'
                else:
                    statuses[email] = 'active'
    except Exception:
        # If fetching fails, default all to unknown
        for email in emails:
            statuses[email] = 'unknown'
    
    active_count = sum(1 for s in statuses.values() if s == 'active')
    expired_count = sum(1 for s in statuses.values() if s == 'expired')
    disabled_count = sum(1 for s in statuses.values() if s == 'disabled')
    unknown_count = sum(1 for s in statuses.values() if s == 'unknown')
    
    text = (
        f"📦 <b>سرویس‌های نمایندگی</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"🟢 فعال: {active_count} | 🔴 منقضی/غیرفعال: {expired_count + disabled_count} | ❓ نامشخص: {unknown_count}\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"نمایش {len(emails)} سرویس اخیر (حداکثر ۵۰).\n\n"
        f"👇 برای جزئیات و مدیریت، سرویس را انتخاب کنید:"
    )
    
    kb_buttons = []
    for email in emails:
        status = statuses.get(email, 'unknown')
        if status == 'active':
            emoji = '🟢'
        elif status in ('expired', 'disabled'):
            emoji = '🔴'
        else:
            emoji = '❓'
        kb_buttons.append([InlineKeyboardButton(text=f"{emoji} {email}", callback_data=f"stat_{email}")])
    
    kb_buttons.append([InlineKeyboardButton(text="⬅️ بازگشت", callback_data="reseller_panel")])
    kb = InlineKeyboardMarkup(inline_keyboard=kb_buttons)
    await callback.message.edit_text(text, reply_markup=kb, parse_mode="HTML")

# ----- Create new reseller service -----
@dp.callback_query(F.data == "res_new")
async def reseller_new_start(callback: types.CallbackQuery, state: FSMContext):
    if not is_reseller(callback.from_user.id):
        return await callback.answer("⛔ دسترسی نمایندگی ندارید.", show_alert=True)
    bal = get_reseller_balance(callback.from_user.id)
    if not bal or bal[0] <= 0:
        return await callback.answer("⚠️ موجودی ترافیک شما کافی نیست. با مدیریت تماس بگیرید.", show_alert=True)
    await callback.message.edit_text(
        "📝 <b>نام اتصال (برای مشتری) را وارد کنید:</b>\n"
        "▫️ فقط حروف و اعداد انگلیسی و _ (مثال: <code>ali_01</code>)",
        reply_markup=get_cancel_kb(), parse_mode="HTML"
    )
    await state.set_state(ResellerFlow.wait_for_name)

@dp.message(ResellerFlow.wait_for_name)
async def reseller_new_name(message: types.Message, state: FSMContext):
    if not is_reseller(message.from_user.id):
        return await state.clear()
    if not re.match(r"^[A-Za-z0-9_]+$", message.text):
        return await message.answer(
            "⚠️ <b>فرمت نامعتبر.</b>\nفقط حروف/اعداد انگلیسی و _ مجاز است (مثال: <code>ali_01</code>):",
            reply_markup=get_cancel_kb(), parse_mode="HTML"
        )
    await state.update_data(client_name=message.text)
    bal = get_reseller_balance(message.from_user.id)
    remaining_str = format_size(max(0, bal[0])) if bal else "0"
    await message.answer(
        f"📶 <b>حجم سرویس را به گیگابایت وارد کنید:</b>\n(فقط عدد، مثال: <b>10</b>)\n\n"
        f"📊 موجودی باقیمانده: <b>{remaining_str}</b>",
        reply_markup=get_cancel_kb(), parse_mode="HTML"
    )
    await state.set_state(ResellerFlow.wait_for_gb)

@dp.message(ResellerFlow.wait_for_gb)
async def reseller_new_gb(message: types.Message, state: FSMContext):
    if not is_reseller(message.from_user.id):
        return await state.clear()
    if not message.text.isdigit() or int(message.text) <= 0:
        return await message.answer("⚠️ لطفاً یک عدد معتبر بزرگتر از 0 وارد کنید.", reply_markup=get_cancel_kb())
    gb = int(message.text)
    needed = gb * GB
    data = await state.get_data()
    client_name = data['client_name']

    # Use a single transaction for reservation + invoice creation
    with SessionLocal() as db:
        success, reservation_data = reserve_reseller_allowance(message.from_user.id, needed, db_session=db)
        if not success:
            db.rollback()
            await state.clear()
            return await message.answer(
                "⚠️ <b>موجودی ترافیک کافی نیست.</b>\n\nبرای این حجم اعتبار کافی ندارید. با مدیریت تماس بگیرید.",
                reply_markup=get_reseller_menu(), parse_mode="HTML"
            )
        # Create invoice within the same transaction
        invoice = Invoice(
            telegram_user_id=message.from_user.id,
            reseller_id=message.from_user.id,
            added_gb=gb,
            total_price=0,
            original_price=0,
            discount_amount=0,
            client_name=client_name,
            action_type="RESELLER_NEW",
            status="PROCESSING",
            reservation_data=json.dumps(reservation_data)  # store deduction details
        )
        db.add(invoice)
        db.commit()
        db.refresh(invoice)
        invoice_id = invoice.id

    tasks.provision_reseller_new.delay(invoice_id)
    await invalidate_user_service_cache(message.from_user.id)
    await state.clear()
    await message.answer(
        f"⏳ <b>در حال ساخت سرویس...</b>\n\n🔖 نام: <code>{client_name}</code>\n📶 حجم: {gb} GB\n\n"
        f"پس از آماده شدن، از «📦 سرویس‌های من» لینک اتصال را دریافت کنید.",
        reply_markup=get_reseller_menu(), parse_mode="HTML"
    )

# ----- Reseller renew / top-up of an owned service -----
@dp.callback_query(F.data.startswith("res_topup_"))
async def reseller_topup_start(callback: types.CallbackQuery, state: FSMContext):
    email = callback.data.split("_", 2)[2]
    if not reseller_owns_email(callback.from_user.id, email):
        return await callback.answer("⛔ این سرویس متعلق به شما نیست.", show_alert=True)
    await state.update_data(res_email=email)
    bal = get_reseller_balance(callback.from_user.id)
    remaining_str = format_size(max(0, bal[0])) if bal else "0"
    await callback.message.edit_text(
        f"➕ <b>افزودن حجم به</b> <code>{email}</code>\n\n"
        f"📶 تعداد گیگابایت را وارد کنید (فقط عدد):\n📊 موجودی باقیمانده: <b>{remaining_str}</b>",
        reply_markup=get_cancel_kb(), parse_mode="HTML"
    )
    await state.set_state(ResellerFlow.wait_for_topup_gb)

@dp.callback_query(F.data.startswith("res_renew_"))
async def reseller_renew_start(callback: types.CallbackQuery, state: FSMContext):
    email = callback.data.split("_", 2)[2]
    if not reseller_owns_email(callback.from_user.id, email):
        return await callback.answer("⛔ این سرویس متعلق به شما نیست.", show_alert=True)
    await state.update_data(res_email=email)
    bal = get_reseller_balance(callback.from_user.id)
    remaining_str = format_size(max(0, bal[0])) if bal else "0"
    with SessionLocal() as db:
        days_setting = db.query(AppSetting).filter(AppSetting.key == 'reseller_service_days').first()
        days = int(days_setting.value) if days_setting else 30
    await callback.message.edit_text(
        f"🔄 <b>تمدید</b> <code>{email}</code>\n\n"
        f"مدت <b>{days} روز</b> به اعتبار زمانی اضافه می‌شود.\n"
        f"📶 حجم اضافه (گیگابایت) را وارد کنید (۰ برای فقط تمدید زمان):\n"
        f"📊 موجودی باقیمانده: <b>{remaining_str}</b>",
        reply_markup=get_cancel_kb(), parse_mode="HTML"
    )
    await state.set_state(ResellerFlow.wait_for_renew_gb)

@dp.callback_query(F.data.startswith("res_extend_"))
async def reseller_extend_start(callback: types.CallbackQuery, state: FSMContext):
    email = callback.data.split("_", 2)[2]
    if not reseller_owns_email(callback.from_user.id, email):
        return await callback.answer("⛔ این سرویس متعلق به شما نیست.", show_alert=True)
    await state.update_data(res_email=email)
    await callback.message.edit_text(
        f"⏳ <b>افزایش مدت سرویس</b> <code>{email}</code>\n\n"
        "تعداد روزی که می‌خواهید اضافه شود را وارد کنید (فقط عدد بزرگتر از صفر):",
        reply_markup=get_cancel_kb(), parse_mode="HTML"
    )
    await state.set_state(ResellerFlow.wait_for_extend_days)

async def _reseller_secondary_action(message: types.Message, state: FSMContext, action_type: str, allow_zero: bool):
    """Shared handler body for reseller topup/renew GB input."""
    if not is_reseller(message.from_user.id):
        return await state.clear()
    text = message.text.strip()
    if not text.isdigit() or (int(text) <= 0 and not allow_zero):
        return await message.answer("⚠️ لطفاً یک عدد معتبر وارد کنید.", reply_markup=get_cancel_kb())
    gb = int(text)
    data = await state.get_data()
    email = data.get('res_email')
    if not email or not reseller_owns_email(message.from_user.id, email):
        await state.clear()
        return await message.answer("⛔ سرویس نامعتبر است.", reply_markup=get_reseller_menu())
    needed = gb * GB

    # Use a single transaction for reservation + invoice creation
    with SessionLocal() as db:
        if needed > 0:
            success, reservation_data = reserve_reseller_allowance(message.from_user.id, needed, db_session=db)
            if not success:
                db.rollback()
                await state.clear()
                return await message.answer(
                    "⚠️ <b>موجودی ترافیک کافی نیست.</b>", reply_markup=get_reseller_menu(), parse_mode="HTML"
                )
        else:
            reservation_data = None

        # Create invoice within the same transaction
        invoice = Invoice(
            telegram_user_id=message.from_user.id,
            reseller_id=message.from_user.id,
            added_gb=gb,
            total_price=0,
            original_price=0,
            discount_amount=0,
            client_name=email,
            action_type=action_type,
            status="PROCESSING",
            reservation_data=json.dumps(reservation_data) if reservation_data else None
        )
        db.add(invoice)
        db.commit()
        db.refresh(invoice)
        invoice_id = invoice.id

    if action_type == "RESELLER_TOPUP":
        tasks.provision_reseller_topup.delay(invoice_id)
        note = f"➕ <b>در حال افزودن {gb} گیگابایت به</b> <code>{email}</code>..."
    else:
        tasks.provision_reseller_renew.delay(invoice_id)
        note = f"🔄 <b>در حال تمدید</b> <code>{email}</code>..."
    await invalidate_user_service_cache(message.from_user.id)
    await state.clear()
    await message.answer(note, reply_markup=get_reseller_menu(), parse_mode="HTML")

@dp.message(ResellerFlow.wait_for_topup_gb)
async def reseller_topup_gb(message: types.Message, state: FSMContext):
    await _reseller_secondary_action(message, state, "RESELLER_TOPUP", allow_zero=False)

@dp.message(ResellerFlow.wait_for_renew_gb)
async def reseller_renew_gb(message: types.Message, state: FSMContext):
    await _reseller_secondary_action(message, state, "RESELLER_RENEW", allow_zero=True)

@dp.message(ResellerFlow.wait_for_extend_days)
async def reseller_extend_days(message: types.Message, state: FSMContext):
    if not is_reseller(message.from_user.id):
        return await state.clear()
    if not message.text.strip().isdigit() or int(message.text.strip()) <= 0:
        return await message.answer("⚠️ لطفاً تعداد روزی معتبر بزرگتر از صفر وارد کنید.", reply_markup=get_cancel_kb())
    days = int(message.text.strip())
    data = await state.get_data()
    email = data.get('res_email')
    if not email or not reseller_owns_email(message.from_user.id, email):
        await state.clear()
        return await message.answer("⛔ سرویس نامعتبر است.", reply_markup=get_reseller_menu())
    with SessionLocal() as db:
        invoice = Invoice(telegram_user_id=message.from_user.id, reseller_id=message.from_user.id,
                          added_gb=0, total_price=0, original_price=0, discount_amount=0,
                          client_name=email, action_type="RESELLER_EXTEND", status="PROCESSING",
                          description=json.dumps({'days': days}))
        db.add(invoice); db.commit(); db.refresh(invoice); invoice_id = invoice.id
    tasks.provision_reseller_extend.delay(invoice_id)
    await invalidate_user_service_cache(message.from_user.id)
    await state.clear()
    await message.answer(f"⏳ در حال افزودن {days} روز به <code>{email}</code>...", reply_markup=get_reseller_menu(), parse_mode="HTML")

# ==============================================================================
# RESELLER TRAFFIC PACK PURCHASE
# ==============================================================================
@dp.callback_query(F.data == "res_buy_pack")
async def reseller_buy_pack(callback: types.CallbackQuery, state: FSMContext):
    if not is_reseller(callback.from_user.id):
        return await callback.answer("⛔ دسترسی نمایندگی ندارید.", show_alert=True)
    with SessionLocal() as db:
        packs = db.query(TrafficPack).filter(TrafficPack.is_active == True).all()
    if not packs:
        return await callback.answer("❌ در حال حاضر هیچ بسته ترافیکی فعالی وجود ندارد.", show_alert=True)
    text = "🛒 <b>بسته‌های ترافیک موجود برای خرید:</b>\n━━━━━━━━━━━━━━━━━━\n"
    kb_buttons = []
    for p in packs:
        text += f"\n<b>{p.name}</b>\n   📶 {p.traffic_gb} GB | ⏳ {p.duration_days} روز | 💰 {p.price:,} تومان\n"
        kb_buttons.append([InlineKeyboardButton(text=f"🛒 خرید {p.name} — {p.price:,} تومان", callback_data=f"res_select_pack_{p.id}")])
    kb_buttons.append([InlineKeyboardButton(text="❌ انصراف", callback_data="cancel")])
    kb = InlineKeyboardMarkup(inline_keyboard=kb_buttons)
    await callback.message.edit_text(text, reply_markup=kb, parse_mode="HTML")

@dp.callback_query(F.data.startswith("res_select_pack_"))
async def reseller_select_pack(callback: types.CallbackQuery, state: FSMContext):
    if not is_reseller(callback.from_user.id):
        return await callback.answer("⛔ دسترسی نمایندگی ندارید.", show_alert=True)
    pack_id = int(callback.data.split("_")[3])
    with SessionLocal() as db:
        pack = db.query(TrafficPack).filter(TrafficPack.id == pack_id, TrafficPack.is_active == True).first()
        if not pack:
            return await callback.answer("❌ بسته یافت نشد یا غیرفعال است.", show_alert=True)
    await state.update_data(pack_id=pack_id, pack_name=pack.name, pack_price=pack.price, pack_gb=pack.traffic_gb, pack_days=pack.duration_days)
    # Ask for coupon
    sent = await callback.message.edit_text(
        "🎫 <b>کد تخفیف دارید؟</b>\n\nاگر کد تخفیف دارید، آن را وارد کنید. در غیر این صورت «رد شدن» را بزنید.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⏭ رد شدن", callback_data="res_skip_coupon_pack")]
        ]),
        parse_mode="HTML"
    )
    await state.update_data(last_bot_msg_id=sent.message_id)
    await state.set_state(ResellerPackFlow.wait_for_coupon)

@dp.callback_query(F.data == "res_skip_coupon_pack", ResellerPackFlow.wait_for_coupon)
async def reseller_skip_coupon_pack(callback: types.CallbackQuery, state: FSMContext):
    await state.update_data(coupon_code=None)
    await cleanup_prev_message(bot, state, callback.message.chat.id)
    await show_pack_payment(callback.message, state)

@dp.message(ResellerPackFlow.wait_for_coupon)
async def reseller_process_coupon_pack(message: types.Message, state: FSMContext):
    if not is_reseller(message.from_user.id):
        return await state.clear()
    code = message.text.strip()
    data = await state.get_data()
    original_price = data['pack_price']
    action = "pack"
    with SessionLocal() as db:
        coupon = db.query(Coupon).filter(Coupon.code == code, Coupon.active == True).first()
        if not coupon:
            await message.answer("❌ کد تخفیف معتبر نیست. لطفاً مجدداً وارد کنید یا رد شوید.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⏭ رد شدن", callback_data="res_skip_coupon_pack")]]))
            return
        if coupon.expiry_date and coupon.expiry_date.replace(tzinfo=timezone.utc) < datetime.now(timezone.utc):
            await message.answer("❌ این کد تخفیف منقضی شده است.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⏭ رد شدن", callback_data="res_skip_coupon_pack")]]))
            return
        total_used = db.query(CouponUsage).filter(CouponUsage.coupon_id == coupon.id).count()
        if coupon.max_uses_total > 0 and total_used >= coupon.max_uses_total:
            await message.answer("❌ این کد تخفیف دیگر قابل استفاده نیست.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⏭ رد شدن", callback_data="res_skip_coupon_pack")]]))
            return
        user_used = db.query(CouponUsage).filter(CouponUsage.coupon_id == coupon.id, CouponUsage.user_id == message.from_user.id).count()
        if coupon.max_uses_per_user > 0 and user_used >= coupon.max_uses_per_user:
            await message.answer("❌ شما قبلاً از این کد تخفیف استفاده کرده‌اید.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⏭ رد شدن", callback_data="res_skip_coupon_pack")]]))
            return
        applicable = coupon.applicable_to
        if applicable != 'all':
            allowed = applicable.split(',')
            if 'pack' not in allowed:
                await message.answer("❌ این کد تخفیف برای خرید بسته ترافیک قابل استفاده نیست.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⏭ رد شدن", callback_data="res_skip_coupon_pack")]]))
                return
        # Apply discount
        final_price, discount, discount_desc = calculate_discount(original_price, action, coupon)
        await state.update_data(coupon_code=code, final_price=final_price, discount_amount=discount, discount_desc=discount_desc)
    await delete_user_message(bot, message)
    await cleanup_prev_message(bot, state, message.chat.id)
    await show_pack_payment(message, state)

async def show_pack_payment(message: types.Message, state: FSMContext):
    """Show payment details for pack purchase."""
    data = await state.get_data()
    pack_name = data.get('pack_name')
    pack_gb = data.get('pack_gb')
    pack_days = data.get('pack_days')
    original_price = data.get('pack_price')
    final_price = data.get('final_price', original_price)
    discount = data.get('discount_amount', 0)
    discount_desc = data.get('discount_desc', '')
    coupon_code = data.get('coupon_code')
    with SessionLocal() as db:
        card_info = db.query(AppSetting).filter(AppSetting.key == "payment_card").first()
        card_text = card_info.value if card_info else "<i>⚠️ اطلاعات پرداخت تنظیم نشده است. به مدیریت اطلاع دهید.</i>"
        size_setting = db.query(AppSetting).filter(AppSetting.key == 'max_receipt_size_mb').first()
        max_size_mb = int(size_setting.value) if size_setting else 10
    msg = (
        f"🛒 <b>خرید بسته ترافیک</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"📦 بسته: <b>{pack_name}</b>\n"
        f"📶 حجم: {pack_gb} GB\n"
        f"⏳ مدت: {pack_days} روز\n"
        f"💰 قیمت اصلی: {original_price:,} تومان\n"
    )
    if discount > 0:
        msg += f"🎫 تخفیف: <span class='tg-spoiler'>-{discount:,} تومان</span> ({discount_desc})\n"
    msg += f"━━━━━━━━━━━━━━━━━━\n"
    msg += f"💵 <b>مبلغ نهایی:</b> <b>{final_price if discount > 0 else original_price:,} تومان</b>\n\n"
    msg += f"📋 <b>اطلاعات پرداخت:</b>\n<blockquote>{card_text}</blockquote>\n"
    msg += (
        f"📌 <b>نکات مهم:</b>\n"
        f"▫️ پس از واریز، <b>عکس رسید</b> را ارسال کنید\n"
        f"▫️ حداکثر حجم مجاز: {max_size_mb} مگابایت\n"
        f"▫️ پس از تایید ادمین، بسته ترافیک به موجودی شما اضافه خواهد شد\n"
    )
    await state.update_data(final_price=final_price, original_price=original_price, discount_amount=discount, discount_desc=discount_desc, coupon_code=coupon_code)
    sent = await message.answer(msg, reply_markup=get_cancel_kb(), parse_mode="HTML")
    await state.update_data(last_bot_msg_id=sent.message_id)
    await state.set_state(ResellerPackFlow.wait_for_receipt)

@dp.message(ResellerPackFlow.wait_for_receipt, F.photo)
async def reseller_pack_receipt(message: types.Message, state: FSMContext):
    if not is_reseller(message.from_user.id):
        return await state.clear()
    await bot.send_chat_action(message.chat.id, "upload_photo")
    # Check file size
    max_size_mb = 10
    with SessionLocal() as db:
        size_setting = db.query(AppSetting).filter(AppSetting.key == 'max_receipt_size_mb').first()
        if size_setting:
            max_size_mb = int(size_setting.value)
    max_bytes = max_size_mb * 1024 * 1024
    if message.photo[-1].file_size > max_bytes:
        await message.answer(f"⚠️ حجم عکس بیش از حد مجاز ({max_size_mb} MB) است. لطفاً عکس را فشرده کنید و دوباره ارسال نمایید.", reply_markup=get_cancel_kb())
        return
    data = await state.get_data()
    photo = message.photo[-1]
    file_path = f"./storage/receipts/{message.from_user.id}_{time.time_ns()}.jpg"
    await bot.download(photo, destination=file_path)
    # Create invoice
    with SessionLocal() as db:
        invoice = Invoice(
            telegram_user_id=message.from_user.id,
            reseller_id=message.from_user.id,
            pack_id=data['pack_id'],
            total_price=data['final_price'],
            original_price=data['original_price'],
            discount_amount=data['discount_amount'],
            coupon_code=data.get('coupon_code'),
            client_name=f"pack_{data['pack_id']}_{message.from_user.id}",  # unique identifier
            action_type="RESELLER_PACK_BUY",
            screenshot_local_path=file_path,
            status="PENDING"
        )
        db.add(invoice)
        db.commit()
        db.refresh(invoice)
        invoice_id = invoice.id
        pack_name = data['pack_name']
        pack_gb = data['pack_gb']
        pack_days = data['pack_days']
        final_price = invoice.total_price
        discount_amount = invoice.discount_amount or 0
        coupon_code = invoice.coupon_code
    # Notify admins
    admin_kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="✅ تایید", callback_data=f"approve_{invoice_id}", style="success"),
        InlineKeyboardButton(text="⛔ رد کردن", callback_data=f"reject_{invoice_id}", style="danger")
    ]])
    buyer_name = message.from_user.full_name or message.from_user.first_name or str(message.from_user.id)
    buyer_username = f"@{message.from_user.username}" if message.from_user.username else "ندارد"
    caption = (
        f"🧾 <b>فاکتور جدید #{invoice_id}</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"👤 خریدار: <b>{buyer_name}</b>\n"
        f"📛 یوزرنیم: <code>{buyer_username}</code>\n"
        f"🆔 شناسه: <code>{message.from_user.id}</code>\n"
        f"📦 بسته: <b>{pack_name}</b>\n"
        f"📶 {pack_gb} GB | ⏳ {pack_days} روز\n"
        f"⚡ نوع عملیات: <b>خرید بسته ترافیک</b>\n"
        f"💰 مبلغ نهایی: <b>{final_price:,} تومان</b>\n"
    )
    if discount_amount > 0:
        caption += f"🎫 تخفیف: <b>{discount_amount:,} تومان</b>\n"
    if coupon_code:
        caption += f"🎫 کد تخفیف: <b>{coupon_code}</b>\n"
    for admin_id in get_admin_ids():
        try:
            await bot.send_photo(admin_id, photo.file_id, caption=caption, reply_markup=admin_kb, parse_mode="HTML")
        except Exception:
            pass
    await cleanup_prev_message(bot, state, message.chat.id)
    await delete_user_message(bot, message)
    await state.clear()
    await message.answer(
        "✅ <b>رسید شما با موفقیت ثبت شد!</b> 🙏\n\n"
        "📋 <b>مراحل بعدی:</b>\n"
        "1️⃣ مدیران سیستم رسید شما را بررسی می‌کنند\n"
        "2️⃣ پس از تایید، بسته ترافیک به موجودی شما اضافه خواهد شد\n\n"
        "⏳ <i>لطفاً شکیبا باشید — حداکثر تا چند ساعت آینده نتیجه اعلام می‌شود.</i>\n"
        "🌟 از اعتماد شما سپاسگزاریم!",
        reply_markup=get_reseller_menu(),
        parse_mode="HTML"
    )

@dp.message(ResellerPackFlow.wait_for_receipt)
async def reseller_pack_receipt_invalid(message: types.Message, state: FSMContext):
    """Catch-all for non-photo messages during receipt state."""
    await message.answer(
        "⚠️ <b>رسید باید به صورت عکس ارسال شود.</b>\n\n"
        "لطفاً اسکرین‌شات یا عکس فیش واریزی را به عنوان <b>عکس</b> ارسال کنید.\n"
        "برای انصراف، دکمه ❌ انصراف را بزنید.",
        reply_markup=get_cancel_kb(),
        parse_mode="HTML"
    )

# ==============================================================================
# RESELLER SERVICE DELETION
# ==============================================================================
@dp.callback_query(F.data.startswith("res_toggle_"))
async def reseller_toggle_service(callback: types.CallbackQuery):
    email = callback.data.split("_", 2)[2]  # res_toggle_{email}
    if not reseller_owns_email(callback.from_user.id, email):
        return await callback.answer("⛔ این سرویس متعلق به شما نیست.", show_alert=True)
    xui = XUIClient()
    full = await xui.get_client_full(email)
    if not full or 'client' not in full:
        return await callback.answer("❌ سرویس در پنل یافت نشد.", show_alert=True)
    client_data = full['client']
    new_enable = not client_data.get('enable', True)
    client_data['enable'] = new_enable
    await xui.update_client(email, client_data)
    status_text = "فعال" if new_enable else "غیرفعال"
    await callback.answer(f"✅ وضعیت سرویس به {status_text} تغییر یافت.", show_alert=True)
    await invalidate_user_service_cache(callback.from_user.id)
    # Re-show the service detail view
    await view_live_stats(callback)

@dp.callback_query(F.data.startswith("res_delete_"))
async def reseller_delete_confirm(callback: types.CallbackQuery):
    email = callback.data.split("_", 2)[2]  # res_delete_{email}
    # Check if the user owns this service
    if not reseller_owns_email(callback.from_user.id, email):
        return await callback.answer("⛔ این سرویس متعلق به شما نیست.", show_alert=True)
    # Get current client stats from panel to show usage
    xui = XUIClient()
    full = await xui.get_client_full(email)
    if not full or 'client' not in full:
        return await callback.answer("❌ سرویس در پنل یافت نشد.", show_alert=True)
    client_data = full['client']
    total_bytes = client_data.get('totalGB', 0)
    used_bytes = full.get('usedTraffic', 0)
    expiry_ms = client_data.get('expiryTime', 0)
    now_ms = int(time.time() * 1000)
    is_expired = expiry_ms > 0 and now_ms > expiry_ms
    if is_expired:
        refund_text = "⚠️ سرویس منقضی شده است، هیچ ترافیکی بازگردانده نمی‌شود."
    else:
        unused = max(0, total_bytes - used_bytes)
        refund_text = f"📤 ترافیک قابل بازگشت: <b>{format_size(unused)}</b>"
    # Show confirmation
    text = (
        f"🗑 <b>حذف سرویس</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"🔖 سرویس: <code>{email}</code>\n"
        f"📊 مصرف فعلی: {format_size(used_bytes)} / {format_size(total_bytes)}\n"
        f"{refund_text}\n"
        f"\n"
        f"⚠️ این عملیات غیرقابل بازگشت است. سرویس غیرفعال می‌شود.\n"
        f"آیا مطمئن هستید؟"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🗑 بله، حذف شود", callback_data=f"confirm_res_delete_{email}")],
        [InlineKeyboardButton(text="❌ انصراف", callback_data=f"stat_{email}")]
    ])
    await callback.message.edit_text(text, reply_markup=kb, parse_mode="HTML")

@dp.callback_query(F.data.startswith("confirm_res_delete_"))
async def confirm_res_delete(callback: types.CallbackQuery):
    email = callback.data.split("_", 3)[3]  # confirm_res_delete_{email}
    if not reseller_owns_email(callback.from_user.id, email):
        return await callback.answer("⛔ این سرویس متعلق به شما نیست.", show_alert=True)
    # Find an invoice for this email to pass to the task
    with SessionLocal() as db:
        inv = db.query(Invoice).filter(
            Invoice.reseller_id == callback.from_user.id,
            Invoice.client_name == email,
            Invoice.status.in_(['COMPLETE', 'PROCESSING'])
        ).first()
        if not inv:
            return await callback.answer("❌ هیچ فاکتور معتبری برای این سرویس یافت نشد.", show_alert=True)
        invoice_id = inv.id
    # Trigger the Celery task
    tasks.delete_reseller_service.delay(invoice_id)
    await invalidate_user_service_cache(callback.from_user.id)
    await callback.message.edit_text(
        f"🗑 <b>در حال حذف سرویس <code>{email}</code>...</b>\n\n"
        f"لطفاً چند لحظه صبر کنید. پس از اتمام، به شما اعلام می‌شود.",
        parse_mode="HTML"
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("user_delete_"))
async def user_delete_confirm(callback: types.CallbackQuery):
    email = callback.data.split("_", 2)[2]  # user_delete_{email}
    # Check if the user owns this service
    if not user_owns_email(callback.from_user.id, email):
        return await callback.answer("⛔ این سرویس متعلق به شما نیست.", show_alert=True)
    # Get current client stats from panel to show usage
    xui = XUIClient()
    full = await xui.get_client_full(email)
    if not full or 'client' not in full:
        return await callback.answer("❌ سرویس در پنل یافت نشد.", show_alert=True)
    client_data = full['client']
    total_bytes = client_data.get('totalGB', 0)
    used_bytes = full.get('usedTraffic', 0)
    expiry_ms = client_data.get('expiryTime', 0)
    now_ms = int(time.time() * 1000)
    is_expired = expiry_ms > 0 and now_ms > expiry_ms
    # Show confirmation
    text = (
        f"🗑 <b>حذف سرویس</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"🔖 سرویس: <code>{email}</code>\n"
        f"📊 مصرف فعلی: {format_size(used_bytes)} / {format_size(total_bytes)}\n"
        f"\n"
        f"⚠️ این عملیات غیرقابل بازگشت است. سرویس غیرفعال می‌شود.\n"
        f"آیا مطمئن هستید؟"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🗑 بله، حذف شود", callback_data=f"confirm_user_delete_{email}")],
        [InlineKeyboardButton(text="❌ انصراف", callback_data=f"stat_{email}")]
    ])
    await callback.message.edit_text(text, reply_markup=kb, parse_mode="HTML")

@dp.callback_query(F.data.startswith("confirm_user_delete_"))
async def confirm_user_delete(callback: types.CallbackQuery):
    email = callback.data.split("_", 3)[3]  # confirm_user_delete_{email}
    if not user_owns_email(callback.from_user.id, email):
        return await callback.answer("⛔ این سرویس متعلق به شما نیست.", show_alert=True)
    # Find an invoice for this email to pass to the task
    with SessionLocal() as db:
        inv = db.query(Invoice).filter(
            Invoice.telegram_user_id == callback.from_user.id,
            Invoice.client_name == email,
            Invoice.status.in_(['COMPLETE', 'PROCESSING'])
        ).first()
        if not inv:
            return await callback.answer("❌ هیچ فاکتور معتبری برای این سرویس یافت نشد.", show_alert=True)
        invoice_id = inv.id
    # Trigger the Celery task
    tasks.delete_user_service.delay(invoice_id)
    await invalidate_user_service_cache(callback.from_user.id)
    await callback.message.edit_text(
        f"🗑 <b>در حال حذف سرویس <code>{email}</code>...</b>\n\n"
        f"لطفاً چند لحظه صبر کنید. پس از اتمام، به شما اعلام می‌شود.",
        parse_mode="HTML"
    )
    await callback.answer()

# ==============================================================================
# MY PLANS
# ==============================================================================
async def my_plans_content(user_id: int):
    """Shared logic for my_plans. Returns (text, reply_markup).
    
    Fetches user's services from the database first (authoritative source),
    then filters to show only services that actually exist on the panel
    (including disabled ones). Services deleted from the panel are not shown.
    Also fetches live status for each service to show active/expired badges.
    Uses Redis caching for service status to minimize panel API calls.
    """
    tg_id = str(user_id)
    cache_key = f"user_emails:{tg_id}"
    cached_emails = await redis_client.get(cache_key)
    if cached_emails:
        emails = json.loads(cached_emails)
    else:
        # 1) Authoritative source: DB — all COMPLETE invoices with a client_name
        with SessionLocal() as db:
            invoices = db.query(Invoice).filter(
                Invoice.telegram_user_id == user_id,
                Invoice.status == "COMPLETE",
                Invoice.client_name.isnot(None),
                Invoice.client_name != ""
            ).all()
        db_emails = list(dict.fromkeys([inv.client_name for inv in invoices]))

        # 2) Filter: Only show emails that actually exist on the panel
        # Use the singleton XUIClient for efficiency
        xui = get_xui_client()
        try:
            # Fetch all clients from panel to check existence
            panel_emails_set = set(await xui.get_group_emails(tg_id))

            # Emails in the group listing are confirmed for free. The rest may
            # still exist on the panel but not be group-assigned, so probe them —
            # in PARALLEL. This used to be a serial loop where each missing/deleted
            # email cost ~3.5s of retry backoff (the reason "My Services" was slow
            # for users with stale services). Combined with the not-found fast-path
            # in XUIClient, the whole check is now ~one round-trip.
            confirmed = {e for e in db_emails if e in panel_emails_set}
            missing = [e for e in db_emails if e not in panel_emails_set]
            if missing:
                probes = await asyncio.gather(
                    *[xui.get_client_full(e) for e in missing],
                    return_exceptions=True,
                )
                for e, info in zip(missing, probes):
                    if not isinstance(info, Exception) and info is not None and 'client' in info:
                        confirmed.add(e)
            # Preserve the original DB order for display.
            emails = [e for e in db_emails if e in confirmed]
        except Exception:
            emails = db_emails

        await redis_client.set(cache_key, json.dumps(emails), ex=120)

    # Amnezia services (separate table) shown below the XUI ones.
    with SessionLocal() as db:
        amz_rows = db.query(AmneziaService).filter(
            AmneziaService.telegram_user_id == user_id,
            AmneziaService.status != 'deleted'
        ).order_by(AmneziaService.id.asc()).all()
    now_utc = datetime.now(timezone.utc)
    amz_items = [{
        "id": s.id,
        "name": s.name,
        "live": s.status == 'active' and (s.expiry_date is None or s.expiry_date > now_utc),
    } for s in amz_rows]

    if not emails and not amz_items:
        return (
            "📦 <b>سرویس‌های من</b>\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"⚠️ شما هیچ سرویس فعالی ندارید.\n\n"
            f"برای شروع می‌توانید:\n"
            f"▫️ یک اشتراک جدید خریداری کنید 🛒\n"
            f"▫️ از تست رایگان استفاده کنید 🎁",
            InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🛒 خرید اشتراک جدید", callback_data="buy_plan"),
                 InlineKeyboardButton(text="🎁 تست رایگان", callback_data="free_trial")],
                [InlineKeyboardButton(text="🏠 منوی اصلی", callback_data="main_menu")]
            ])
        )
    
    # Check cache for each email's status
    status_cache_key = f"service_status:{tg_id}"
    cached_statuses = await redis_client.get(status_cache_key)
    statuses = {}
    emails_to_fetch = []
    
    if cached_statuses:
        try:
            cached_data = json.loads(cached_statuses)
            # Verify cache is still fresh (60 second TTL)
            for email in emails:
                if email in cached_data:
                    statuses[email] = cached_data[email]
                else:
                    emails_to_fetch.append(email)
        except Exception:
            emails_to_fetch = emails[:]
    else:
        emails_to_fetch = emails[:]
    
    # Fetch uncached statuses using a single shared XUIClient singleton
    if emails_to_fetch:
        xui = get_xui_client()
        try:
            fetch_tasks = [xui.get_client_full(email) for email in emails_to_fetch]
            results = await asyncio.gather(*fetch_tasks, return_exceptions=True)
            
            for email, result in zip(emails_to_fetch, results):
                if isinstance(result, Exception) or result is None or 'client' not in result:
                    statuses[email] = 'unknown'
                else:
                    client = result['client']
                    expiry = client.get('expiryTime', 0)
                    now_ms = int(time.time() * 1000)
                    if expiry > 0 and now_ms > expiry:
                        statuses[email] = 'expired'
                    elif not client.get('enable', True):
                        statuses[email] = 'disabled'
                    else:
                        statuses[email] = 'active'
        finally:
            pass  # Don't close the singleton client
        
        # Cache the newly fetched statuses (60 second TTL)
        try:
            await redis_client.set(status_cache_key, json.dumps(statuses), ex=60)
        except Exception:
            pass  # Cache failure should not break the response
    
    # Ensure all emails have a status (default to unknown if missing)
    for email in emails:
        if email not in statuses:
            statuses[email] = 'unknown'
    
    active_count = sum(1 for s in statuses.values() if s == 'active')
    expired_count = sum(1 for s in statuses.values() if s == 'expired')
    disabled_count = sum(1 for s in statuses.values() if s == 'disabled')
    unknown_count = sum(1 for s in statuses.values() if s == 'unknown')
    active_count += sum(1 for a in amz_items if a["live"])
    expired_count += sum(1 for a in amz_items if not a["live"])

    text = (
        f"📦 <b>سرویس‌های من</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"🟢 فعال: {active_count} | 🔴 منقضی/غیرفعال: {expired_count + disabled_count} | ❓ نامشخص: {unknown_count}\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"👇 برای مشاهده جزئیات، سرویس مورد نظر را انتخاب کنید:\n\n"
        f"📊 <b>تاریخچه خرید و وضعیت سرویس‌ها:</b>\n"
        f"برای مشاهده لیست خریدها و سرویس‌های حذف‌شده، از دکمه زیر استفاده کنید:"
    )
    
    kb_buttons = []
    for email in emails:
        status = statuses.get(email, 'unknown')
        if status == 'active':
            emoji = '🟢'
        elif status in ('expired', 'disabled'):
            emoji = '🔴'
        else:
            emoji = '❓'
        kb_buttons.append([InlineKeyboardButton(text=f"{emoji} {email}", callback_data=f"stat_{email}")])

    for a in amz_items:
        emoji = '🟢' if a["live"] else '🔴'
        kb_buttons.append([InlineKeyboardButton(text=f"🟣{emoji} {a['name']}", callback_data=f"amzstat_{a['id']}")])

    # Add user logs button
    kb_buttons.append([InlineKeyboardButton(text="📜 تاریخچه خرید و سرویس‌های حذف‌شده", callback_data="user_logs")])
    kb_buttons.append([InlineKeyboardButton(text="🏠 منوی اصلی", callback_data="main_menu")])
    kb = InlineKeyboardMarkup(inline_keyboard=kb_buttons)
    
    return text, kb

@dp.callback_query(F.data == "my_plans")
async def my_plans_cb(callback: types.CallbackQuery):
    text, kb = await my_plans_content(callback.from_user.id)
    await callback.message.edit_text(text, reply_markup=kb, parse_mode="HTML")

@dp.callback_query(F.data.startswith("stat_"))
async def view_live_stats(callback: types.CallbackQuery):
    email = callback.data.split("_", 1)[1]
    xui = get_xui_client()
    try:
        full_client_response = await xui.get_client_full(email)
    except Exception as e:
        if "record not found" in str(e).lower():
            # Client not found in panel
            await callback.answer("❌ سرویس مورد نظر در پنل یافت نشد.", show_alert=True)
            # Determine back button based on user type
            if is_reseller(callback.from_user.id):
                back_callback = "res_list"
            else:
                back_callback = "my_plans"
            await callback.message.edit_text(
                "❌ <b>سرویس مورد نظر در پنل یافت نشد.</b>\n\nاین سرویس احتمالاً قبلاً حذف شده است.",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="⬅️ بازگشت", callback_data=back_callback)]
                ]),
                parse_mode="HTML"
            )
            return
        else:
            raise
    if not full_client_response or 'client' not in full_client_response: 
        return await callback.answer("❌ خطا: کلاینت مورد نظر در پنل یافت نشد.", show_alert=True)
    
    client_data = full_client_response.get('client', {})
    client_enabled = client_data.get('enable', True)
    used_bytes = full_client_response.get('usedTraffic', 0)
    total_bytes = client_data.get('totalGB', 0)
    exp_time = client_data.get('expiryTime', 0)
    now_ms = int(time.time() * 1000)
    
    # Online status
    last_online_data = await xui.get_last_online()
    last_seen_ms = last_online_data.get(email)
    if last_seen_ms:
        last_seen_str = datetime.fromtimestamp(last_seen_ms/1000).strftime('%Y-%m-%d %H:%M')
        if (now_ms - last_seen_ms) < 120000:
            online_badge = "🟢 آنلاین"
        else:
            online_badge = f"⚪ آخرین بازدید: {last_seen_str}"
    else:
        online_badge = "⚪ هنوز متصل نشده"
    
    # Traffic
    used_str = format_size(used_bytes)
    total_str = format_size(total_bytes) if total_bytes > 0 else "♾️ نامحدود"
    remaining_bytes = max(0, total_bytes - used_bytes) if total_bytes > 0 else 0
    remaining_str = format_size(remaining_bytes) if total_bytes > 0 else "♾️"
    progress_bar = get_progress_bar(used_bytes, total_bytes) if total_bytes > 0 else "♾️"
    
    # Expiry
    remaining_time = format_expiry_remaining(exp_time)
    exp_str = datetime.fromtimestamp(exp_time/1000).strftime('%Y-%m-%d') if exp_time > 0 else "♾️ نامحدود"
    
    # Status badge
    is_expired = exp_time > 0 and now_ms > exp_time
    if is_expired:
        status_badge = "🔴 منقضی شده"
    elif not client_enabled:
        status_badge = "🔴 غیرفعال"
    else:
        status_badge = "🟢 فعال"
    
    text = (
        f"📊 <b>داشبورد سرویس</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"🔖 <b>نام:</b> <code>{email}</code>\n"
        f"📌 <b>وضعیت:</b> {status_badge} | {online_badge}\n"
        f"\n"
        f"📈 <b>مصرف داده:</b>\n"
        f"{progress_bar}\n"
        f"📥 <b>مصرف شده:</b> {used_str}\n"
        f"📦 <b>باقیمانده:</b> {remaining_str} از {total_str}\n"
        f"\n"
        f"⏳ <b>زمان باقیمانده:</b> {remaining_time}\n"
        f"📅 <b>تاریخ انقضا:</b> {exp_str}\n"
        f"━━━━━━━━━━━━━━━━━━"
    )
    
    kb_buttons = [
        [InlineKeyboardButton(text="🔗 لینک ساب (اصلی)", callback_data=f"getlink_{email}")],
        [InlineKeyboardButton(text="🔗 لینک‌های مستقیم (V2ray)", callback_data=f"rawlink_{email}")],
        [InlineKeyboardButton(text="🔒 بررسی دستگاه‌های متصل", callback_data=f"ips_{email}")]
    ]
    
    # Smart suggestion: if expiring soon and low traffic, suggest combined action
    if exp_time > 0:
        days_until_expiry = (exp_time - now_ms) / (86400 * 1000)
        if 0 <= days_until_expiry <= 5 and remaining_bytes < 5 * 1024**3:
            text += "\n\n💡 <b>پیشنهاد:</b> سرویس شما به زودی منقضی می‌شود و حجم کمی باقی مانده. بهتر است همزمان تمدید و حجم اضافه خریداری کنید.\n"
    
    # Renew/ top-up buttons
    action_buttons = []
    if exp_time > 0:
        days_until_expiry = (exp_time - now_ms) / (86400 * 1000)
        if 0 <= days_until_expiry <= 5:
            action_buttons.append(InlineKeyboardButton(text="🔄 تمدید سرویس", callback_data=f"renew_{email}"))
        elif days_until_expiry > 5 and remaining_bytes < 5 * 1024**3:
            # Low traffic but not expiring soon — still allow topup
            pass
    if not is_expired:
        action_buttons.append(InlineKeyboardButton(text="➕ خرید حجم اضافه", callback_data=f"topup_{email}"))
    if action_buttons:
        kb_buttons.append(action_buttons)
    # If the viewer is the reseller who owns this config, offer allowance-funded actions + reseller list
    if reseller_owns_email(callback.from_user.id, email):
        res_actions = [InlineKeyboardButton(text="➕ حجم (نمایندگی)", callback_data=f"res_topup_{email}")]
        if not is_expired:
            res_actions.append(InlineKeyboardButton(text="🔄 تمدید (نمایندگی)", callback_data=f"res_renew_{email}"))
        kb_buttons.append(res_actions)
        kb_buttons.append([InlineKeyboardButton(text="⏳ افزایش روزهای سرویس", callback_data=f"res_extend_{email}")])
        # Add toggle enable/disable button for resellers
        toggle_text = "🔒 غیرفعال کردن" if client_enabled else "🟢 فعال کردن"
        kb_buttons.append([InlineKeyboardButton(text=toggle_text, callback_data=f"res_toggle_{email}")])
        # Add delete button for resellers (if service is not already DELETED)
        kb_buttons.append([InlineKeyboardButton(text="🗑 حذف سرویس", callback_data=f"res_delete_{email}")])
        kb_buttons.append([InlineKeyboardButton(text="⬅️ بازگشت به سرویس‌های نمایندگی", callback_data="res_list")])
    # If the viewer is the regular user who owns this config, offer delete button
    if user_owns_email(callback.from_user.id, email):
        kb_buttons.append([InlineKeyboardButton(text="🗑 حذف سرویس", callback_data=f"user_delete_{email}")])
    kb_buttons.append([InlineKeyboardButton(text="⬅️ بازگشت به لیست", callback_data="my_plans")])
    
    await callback.message.edit_text(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_buttons))

@dp.callback_query(F.data.startswith("getlink_"))
async def get_sub_link(callback: types.CallbackQuery):
    email = callback.data.split("_", 1)[1]
    xui = get_xui_client()
    full_client_response = await xui.get_client_full(email)
    if not full_client_response or 'client' not in full_client_response:
        return await callback.answer("خطا: کلاینت مورد نظر یافت نشد.", show_alert=True)
    sub_id = full_client_response['client'].get('subId')
    if not sub_id: return await callback.answer("خطا: شناسه اتصال وجود ندارد.", show_alert=True)
    with SessionLocal() as db:
        base_link_setting = db.query(AppSetting).filter(AppSetting.key == "sub_base_link").first()
        if not base_link_setting or not base_link_setting.value:
            return await callback.answer("⚠️ لینک پایه توسط ادمین تنظیم نشده است.", show_alert=True)
        base_url = base_link_setting.value.strip()
    if not base_url.endswith('/'): base_url += '/'
    final_sub_url = f"{base_url}{sub_id}"
    text = f"""🔗 <b>لینک اتصال (Subscription) شما</b>
━━━━━━━━━━━━━━━━━━
<code>{final_sub_url}</code>
━━━━━━━━━━━━━━━━━━
💡 <i>برای کپی شدن لینک، روی کادر بالا ضربه بزنید.</i>"""
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ بازگشت", callback_data=f"stat_{email}")]])
    await callback.message.edit_text(text, reply_markup=kb, parse_mode="HTML")

@dp.callback_query(F.data.startswith("rawlink_"))
async def show_raw_links(callback: types.CallbackQuery):
    email = callback.data.split("_", 1)[1]
    xui = get_xui_client()
    links = await xui.get_client_links(email)
    if not links:
        return await callback.answer("هیچ لینک مستقیمی برای این سرویس تولید نشده است.", show_alert=True)
    text = "🔗 <b>لینک‌های اتصال مستقیم شما:</b>\n\n"
    for idx, link in enumerate(links[:5]):
        text += f"<code>{link}</code>\n\n"
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ بازگشت", callback_data=f"stat_{email}")]])
    await callback.message.edit_text(text, parse_mode="HTML", reply_markup=kb)

@dp.callback_query(F.data.startswith("ips_"))
async def show_connected_ips(callback: types.CallbackQuery):
    email = callback.data.split("_", 1)[1]
    xui = get_xui_client()
    ips = await xui.get_client_ips(email)
    if not ips:
        text = "✅ <b>هیچ دستگاه/آی‌پی فعالی در حال حاضر متصل نیست.</b>"
    else:
        text = "🔒 <b>لیست آی‌پی‌های متصل اخیراً:</b>\n\n"
        for ip in ips: text += f"▪️ <code>{ip}</code>\n"
        text += "\n<i>اگر دچار مشکل قفل IP شده‌اید، دکمه آزادسازی را بزنید.</i>"
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🧹 آزادسازی آی‌پی‌ها", callback_data=f"clearips_{email}")],
        [InlineKeyboardButton(text="⬅️ بازگشت", callback_data=f"stat_{email}")]
    ])
    await callback.message.edit_text(text, parse_mode="HTML", reply_markup=kb)

@dp.callback_query(F.data.startswith("clearips_"))
async def clear_ips_action(callback: types.CallbackQuery):
    email = callback.data.split("_", 1)[1]
    xui = get_xui_client()
    await xui.clear_client_ips(email)
    await callback.answer("✅ لیست آی‌پی‌ها پاکسازی شد. اکنون دوباره متصل شوید.", show_alert=True)
    await show_connected_ips(callback)

# ==============================================================================
# ADMIN: RETRY STUCK INVOICES
# ==============================================================================
@dp.callback_query(F.data == "admin_retry_invoices")
async def admin_retry_invoices(callback: types.CallbackQuery):
    if callback.from_user.id not in get_admin_ids(): return
    with SessionLocal() as db:
        stuck = db.query(Invoice).filter(Invoice.status.in_(["LOCKED", "PROCESSING", "NEEDS_REVIEW"])).all()
    
    if not stuck:
        return await callback.message.edit_text(
            "✅ <b>همه فاکتورها در وضعیت عادی هستند.</b>\n\nهیچ فاکتور گیرکرده‌ای یافت نشد.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ بازگشت", callback_data="admin_panel")]]),
            parse_mode="HTML"
        )
    
    text = f"📋 <b>فاکتورهای نیازمند بررسی</b>\n━━━━━━━━━━━━━━━━━━\nتعداد: {len(stuck)}\n\n"
    kb_buttons = []
    for inv in stuck:
        status_emoji = "🔒" if inv.status == "LOCKED" else ("❓" if inv.status == "NEEDS_REVIEW" else "⏳")
        text += f"{status_emoji} #{inv.id} — {inv.status} — {inv.action_type} — کاربر {inv.telegram_user_id}\n"
        kb_buttons.append([InlineKeyboardButton(text=f"{status_emoji} برگرداندن #{inv.id} ({inv.status})", callback_data=f"unstick_{inv.id}")])
    kb_buttons.append([InlineKeyboardButton(text="⬅️ بازگشت به پنل", callback_data="admin_panel")])
    
    await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_buttons), parse_mode="HTML")

@dp.callback_query(F.data == "admin_dashboard")
async def admin_dashboard(callback: types.CallbackQuery):
    if callback.from_user.id not in get_admin_ids():
        return await callback.answer("⛔ دسترسی غیرمجاز", show_alert=True)
    await callback.message.edit_text("⏳ <b>در حال بارگذاری داشبورد...</b>", parse_mode="HTML")
    try:
        # DB stats
        with SessionLocal() as db:
            # Total unique users
            user_ids = {row[0] for row in db.query(ReferralCode.telegram_user_id).all()}
            user_ids.update(row[0] for row in db.query(Invoice.telegram_user_id).distinct().all())
            total_users = len(user_ids)
            pending_invoices = db.query(Invoice).filter(Invoice.status == "PENDING").count()
            today = datetime.now(timezone.utc).date()
            today_start = datetime(today.year, today.month, today.day, tzinfo=timezone.utc)
            today_revenue = db.query(Invoice).filter(
                Invoice.status == "COMPLETE",
                Invoice.created_at >= today_start
            ).with_entities(func.sum(Invoice.total_price)).scalar() or 0
            # Get main inbound ID setting
            main_inbound_setting = db.query(AppSetting).filter(AppSetting.key == "main_inbound_id").first()
            main_inbound_id = int(main_inbound_setting.value) if main_inbound_setting and main_inbound_setting.value.isdigit() else None
        # Panel stats
        xui = XUIClient()
        inbounds = await xui.get_enabled_inbounds()
        # Filter by main inbound if set
        if main_inbound_id is not None:
            filtered_inbounds = [ib for ib in inbounds if ib.get('id') == main_inbound_id]
            if not filtered_inbounds:
                # If the selected inbound not found, fallback to all
                filtered_inbounds = inbounds
        else:
            filtered_inbounds = inbounds

        total_clients = 0
        active_clients = 0
        client_emails = set()
        for inbound in filtered_inbounds:
            clients = inbound.get('settings', {}).get('clients', [])
            for client in clients:
                total_clients += 1
                if client.get('enable', False):
                    active_clients += 1
                if client.get('email'):
                    client_emails.add(client['email'])

        # Online clients (only those belonging to the selected inbound)
        online_data = await xui.get_last_online()
        online_count = 0
        for email in online_data:
            if email in client_emails:
                online_count += 1

        # Server status
        status = await xui.get_server_status()
        cpu = status.get('cpu', 0)
        mem_cur = status.get('mem', {}).get('current', 0) / (1024**3)
        mem_tot = status.get('mem', {}).get('total', 1) / (1024**3)
        disk_cur = status.get('disk', {}).get('current', 0) / (1024**3)
        disk_tot = status.get('disk', {}).get('total', 1) / (1024**3)
        xray_state = status.get('xray', {}).get('state', 'unknown')
        xray_status = '🟢 فعال' if xray_state == 'running' else '🔴 متوقف'

        text = (
            f"📊 <b>داشبورد مدیریت</b>\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"👥 <b>تعداد کاربران:</b> {total_users}\n"
            f"📝 <b>فاکتورهای در انتظار:</b> {pending_invoices}\n"
            f"💰 <b>درآمد امروز:</b> {today_revenue:,} تومان\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"🖥 <b>وضعیت سرور</b>\n"
            f"⚙️ Xray: {xray_status}\n"
            f"🧠 CPU: {get_progress_bar(cpu, 100, 8)} {cpu:.1f}%\n"
            f"🗂 RAM: {get_progress_bar(mem_cur, mem_tot, 8)} {mem_cur:.1f}/{mem_tot:.1f} GB\n"
            f"💾 Disk: {get_progress_bar(disk_cur, disk_tot, 8)} {disk_cur:.1f}/{disk_tot:.1f} GB\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"🔌 <b>سرویس‌های پنل</b>\n"
            f"📦 کل سرویس‌ها: {total_clients}\n"
            f"🟢 فعال: {active_clients}\n"
            f"🟠 آنلاین (اخیر): {online_count}\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"👇 <b>اقدامات سریع:</b>"
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ بررسی فاکتورهای در انتظار", callback_data="admin_retry_invoices")],
            [InlineKeyboardButton(text="📢 ارسال پیام همگانی", callback_data="admin_broadcast")],
            [InlineKeyboardButton(text="🔄 ری‌استارت Xray", callback_data="confirm_restart_xray")],
            [InlineKeyboardButton(text="💾 بکاپ", callback_data="admin_backup_tg")],
            [InlineKeyboardButton(text="⬅️ بازگشت به پنل", callback_data="admin_panel")]
        ])
        await callback.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    except Exception as e:
        await callback.message.edit_text(
            f"❌ <b>خطا در بارگذاری داشبورد:</b>\n<code>{str(e)[:200]}</code>\n\nلطفاً دوباره تلاش کنید.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ بازگشت", callback_data="admin_panel")]]),
            parse_mode="HTML"
        )

@dp.callback_query(F.data == "admin_select_inbound")
async def admin_select_inbound(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in get_admin_ids():
        return await callback.answer("⛔ دسترسی غیرمجاز", show_alert=True)
    await callback.message.edit_text("⏳ <b>در حال دریافت لیست سرورها...</b>", parse_mode="HTML")
    try:
        xui = XUIClient()
        inbounds = await xui.get_enabled_inbounds()
        if not inbounds:
            return await callback.message.edit_text(
                "❌ هیچ سروری یافت نشد.",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ بازگشت", callback_data="admin_panel")]]),
                parse_mode="HTML"
            )
        # Get current selection
        with SessionLocal() as db:
            setting = db.query(AppSetting).filter(AppSetting.key == "main_inbound_id").first()
            current_id = int(setting.value) if setting and setting.value.isdigit() else None

        kb_buttons = []
        for ib in inbounds:
            remark = ib.get('remark', f"ID {ib['id']}")
            protocol = ib.get('protocol', '')
            port = ib.get('port', '')
            label = f"{remark} ({protocol}:{port})"
            if ib['id'] == current_id:
                label = "✅ " + label
            kb_buttons.append([InlineKeyboardButton(text=label, callback_data=f"inbound_select_{ib['id']}")])
        kb_buttons.append([InlineKeyboardButton(text="🗑 حذف انتخاب", callback_data="inbound_select_clear")])
        kb_buttons.append([InlineKeyboardButton(text="❌ انصراف", callback_data="cancel")])
        kb = InlineKeyboardMarkup(inline_keyboard=kb_buttons)
        await callback.message.edit_text(
            "🎯 <b>انتخاب سرور اصلی برای داشبورد</b>\n"
            "سروری را که می‌خواهید آمار آن در داشبورد نمایش داده شود، انتخاب کنید.\n"
            "اگر هیچ‌کدام انتخاب نشود، آمار همه سرورها نشان داده می‌شود.\n\n"
            "✅ = انتخاب فعلی",
            reply_markup=kb,
            parse_mode="HTML"
        )
        await state.set_state(AdminFlow.wait_for_select_inbound)
    except Exception as e:
        await callback.message.edit_text(
            f"❌ <b>خطا:</b>\n<code>{str(e)[:200]}</code>",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ بازگشت", callback_data="admin_panel")]]),
            parse_mode="HTML"
        )

@dp.callback_query(AdminFlow.wait_for_select_inbound, F.data.startswith("inbound_select_"))
async def inbound_select_callback(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in get_admin_ids():
        return await callback.answer("⛔ دسترسی غیرمجاز", show_alert=True)
    inbound_id = int(callback.data.split("_")[2])
    with SessionLocal() as db:
        setting = db.query(AppSetting).filter(AppSetting.key == "main_inbound_id").first()
        if setting:
            setting.value = str(inbound_id)
        else:
            db.add(AppSetting(key="main_inbound_id", value=str(inbound_id)))
        db.commit()
    await state.clear()
    await callback.answer(f"✅ سرور {inbound_id} به عنوان سرور اصلی انتخاب شد.", show_alert=True)
    await callback.message.edit_text(
        f"✅ <b>سرور {inbound_id} با موفقیت به عنوان سرور اصلی انتخاب شد.</b>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ بازگشت به پنل", callback_data="admin_panel")]]),
        parse_mode="HTML"
    )

@dp.callback_query(AdminFlow.wait_for_select_inbound, F.data == "inbound_select_clear")
async def inbound_select_clear(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in get_admin_ids():
        return await callback.answer("⛔ دسترسی غیرمجاز", show_alert=True)
    with SessionLocal() as db:
        setting = db.query(AppSetting).filter(AppSetting.key == "main_inbound_id").first()
        if setting:
            db.delete(setting)
            db.commit()
    await state.clear()
    await callback.answer("✅ انتخاب سرور اصلی حذف شد. اکنون آمار همه سرورها نشان داده می‌شود.", show_alert=True)
    await callback.message.edit_text(
        "✅ <b>انتخاب سرور اصلی حذف شد.</b>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ بازگشت به پنل", callback_data="admin_panel")]]),
        parse_mode="HTML"
    )

@dp.callback_query(F.data.startswith("unstick_"))
async def admin_unstick_invoice(callback: types.CallbackQuery):
    if callback.from_user.id not in get_admin_ids(): return
    invoice_id = int(callback.data.split("_")[1])
    with SessionLocal() as db:
        invoice = db.query(Invoice).filter(Invoice.id == invoice_id).first()
        if invoice and invoice.status in ("LOCKED", "PROCESSING", "NEEDS_REVIEW"):
            invoice.status = "PENDING"
            db.commit()
            await callback.answer(f"✅ فاکتور #{invoice_id} به وضعیت PENDING بازگردانده شد.", show_alert=True)
        elif invoice:
            await callback.answer(f"⚠️ وضعیت فعلی: {invoice.status} — قابل برگرداندن نیست.", show_alert=True)
        else:
            await callback.answer("❌ فاکتور یافت نشد.", show_alert=True)
    await admin_retry_invoices(callback)

# ==============================================================================
# ADMIN PANEL & ORIGINAL ADMIN HANDLERS
# ==============================================================================
@dp.callback_query(F.data == "admin_panel")
async def show_admin_panel(callback: types.CallbackQuery):
    if callback.from_user.id not in get_admin_ids(): return
    await callback.message.edit_text("⚙️ <b>پنل مدیریت</b>", reply_markup=get_admin_menu(), parse_mode="HTML")

@dp.callback_query(F.data.startswith("admcat_"))
async def admin_category_open(callback: types.CallbackQuery, state: FSMContext):
    """Open one admin category submenu. The category buttons are pure
    navigation — every action keeps its original callback, so nothing else
    needed to change."""
    if callback.from_user.id not in get_admin_ids(): return
    await state.clear()
    title, kb = get_admin_category_kb(callback.data)
    await callback.message.edit_text(f"⚙️ <b>پنل مدیریت</b>\n{title}", reply_markup=kb, parse_mode="HTML")

@dp.callback_query(F.data == "admin_sys_status")
async def admin_sys_status(callback: types.CallbackQuery):
    if callback.from_user.id not in get_admin_ids(): return
    xui = XUIClient()
    status = await xui.get_server_status()
    cpu = status.get('cpu', 0)
    mem_cur = status.get('mem', {}).get('current', 0) / (1024**3)
    mem_tot = status.get('mem', {}).get('total', 1) / (1024**3)
    disk_cur = status.get('disk', {}).get('current', 0) / (1024**3)
    disk_tot = status.get('disk', {}).get('total', 1) / (1024**3)
    xray_state = status.get('xray', {}).get('state', 'unknown')
    tcp = status.get('tcpCount', 0)
    
    cpu_bar = get_progress_bar(cpu, 100, 8)
    mem_bar = get_progress_bar(mem_cur, mem_tot, 8)
    disk_bar = get_progress_bar(disk_cur, disk_tot, 8)
    
    xray_status = '🟢 فعال' if xray_state == 'running' else '🔴 متوقف'
    
    text = (
        f"🖥 <b>مانیتورینگ سرور</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"⚙️ <b>Xray:</b> {xray_status}\n"
        f"\n"
        f"🧠 <b>پردازنده (CPU):</b>\n{cpu_bar}\n"
        f"   مصرف: {cpu:.1f}%\n"
        f"\n"
        f"🗂 <b>حافظه (RAM):</b>\n{mem_bar}\n"
        f"   {mem_cur:.1f}GB / {mem_tot:.1f}GB\n"
        f"\n"
        f"💾 <b>دیسک:</b>\n{disk_bar}\n"
        f"   {disk_cur:.1f}GB / {disk_tot:.1f}GB\n"
        f"\n"
        f"🔌 <b>اتصالات همزمان (TCP):</b> {tcp}\n"
        f"━━━━━━━━━━━━━━━━━━"
    )
    await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 بروزرسانی", callback_data="admin_sys_status")],
        [InlineKeyboardButton(text="⬅️ بازگشت به پنل", callback_data="admin_panel")]
    ]), parse_mode="HTML")

@dp.callback_query(F.data == "admin_del_depleted")
async def admin_del_depleted(callback: types.CallbackQuery):
    if callback.from_user.id not in get_admin_ids(): return
    xui = XUIClient()
    try:
        await callback.message.edit_text(
            "🗑 <b>در حال پاکسازی کلاینت‌های منقضی...</b>\n\nلطفاً چند لحظه صبر کنید.",
            parse_mode="HTML"
        )
        res = await xui.delete_depleted_clients()
        deleted = res.get('deleted', 0)
        if deleted > 0:
            msg = f"✅ <b>عملیات پاکسازی انجام شد.</b>\n\n{deleted} کلاینت منقضی شده حذف شدند."
        else:
            msg = "✅ <b>هیچ کلاینتی برای پاکسازی وجود نداشت.</b>"
        await callback.message.edit_text(msg, reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ بازگشت به پنل", callback_data="admin_panel")]
        ]), parse_mode="HTML")
    except Exception as e:
        await callback.message.edit_text(f"❌ <b>خطا در پاکسازی:</b> {str(e)[:100]}", 
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ بازگشت", callback_data="admin_panel")]]),
            parse_mode="HTML")

@dp.callback_query(F.data == "admin_backup_tg")
async def admin_backup_tg(callback: types.CallbackQuery):
    if callback.from_user.id not in get_admin_ids(): return
    xui = XUIClient()
    try:
        await callback.message.edit_text(
            "💾 <b>در حال ارسال بکاپ...</b>\n\nلطفاً چند لحظه صبر کنید.",
            parse_mode="HTML"
        )
        await xui.backup_to_telegram()
        await callback.message.edit_text(
            "✅ <b>دستور بکاپ با موفقیت صادر شد.</b>\n\nبکاپ دیتابیس به ادمین‌های تنظیم شده در پنل ارسال می‌شود.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ بازگشت", callback_data="admin_panel")]]),
            parse_mode="HTML"
        )
    except Exception:
        await callback.message.edit_text(
            "❌ <b>خطا در صدور دستور بکاپ.</b>\n\nلطفاً از تنظیمات پنل اطمینان حاصل کنید.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ بازگشت", callback_data="admin_panel")]]),
            parse_mode="HTML"
        )

@dp.callback_query(F.data == "admin_restart_menu")
async def admin_restart_menu(callback: types.CallbackQuery):
    if callback.from_user.id not in get_admin_ids(): return
    text = (
        "⚠️ <b>بخش خطرناک — عملیات ری‌استارت</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"این عملیات باعث قطع سرویس کاربران می‌شود.\n"
        f"لطفاً با احتیاط اقدام کنید.\n\n"
        f"▫️ <b>ری‌استارت Xray:</b> فقط سرویس Xray ری‌استارت می‌شود (اتصال کاربران قطع می‌شود)\n"
        f"▫️ <b>ری‌استارت پنل:</b> کل پنل 3x-ui ری‌استارت می‌شود (چند ثانیه قطعی)"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⚙️ ری‌استارت Xray", callback_data="confirm_restart_xray")],
        [InlineKeyboardButton(text="🔥 ری‌استارت کامل پنل", callback_data="confirm_restart_panel")],
        [InlineKeyboardButton(text="⬅️ بازگشت به پنل", callback_data="admin_panel")]
    ])
    await callback.message.edit_text(text, reply_markup=kb, parse_mode="HTML")

@dp.callback_query(F.data == "confirm_restart_xray")
async def confirm_restart_xray(callback: types.CallbackQuery):
    if callback.from_user.id not in get_admin_ids(): return
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ بله، ری‌استارت کن", callback_data="do_restart_xray")],
        [InlineKeyboardButton(text="❌ خیر، انصراف", callback_data="admin_restart_menu")]
    ])
    await callback.message.edit_text(
        "⚠️ <b>آیا از ری‌استارت Xray مطمئن هستید؟</b>\n\n"
        "این عملیات باعث قطع لحظه‌ای اتصال همه کاربران می‌شود.\n\n"
        "آیا ادامه می‌دهید؟",
        reply_markup=kb, parse_mode="HTML"
    )

@dp.callback_query(F.data == "confirm_restart_panel")
async def confirm_restart_panel(callback: types.CallbackQuery):
    if callback.from_user.id not in get_admin_ids(): return
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ بله، ری‌استارت کن", callback_data="do_restart_panel")],
        [InlineKeyboardButton(text="❌ خیر، انصراف", callback_data="admin_restart_menu")]
    ])
    await callback.message.edit_text(
        "⚠️ <b>آیا از ری‌استارت کامل پنل مطمئن هستید؟</b>\n\n"
        "این عملیات باعث قطع شدن کامل سرویس‌ها برای چند دقیقه می‌شود.\n"
        "ربات تا زمان راه‌اندازی مجدد پنل ممکن است پاسخگو نباشد.\n\n"
        "آیا ادامه می‌دهید؟",
        reply_markup=kb, parse_mode="HTML"
    )

@dp.callback_query(F.data == "do_restart_xray")
async def do_restart_xray(callback: types.CallbackQuery):
    if callback.from_user.id not in get_admin_ids(): return
    xui = XUIClient()
    await callback.message.edit_text(
        "⚙️ <b>در حال ری‌استارت Xray...</b>\n\nلطفاً صبر کنید...",
        parse_mode="HTML"
    )
    await xui.restart_xray()
    await callback.message.edit_text(
        "✅ <b>Xray با موفقیت ری‌استارت شد.</b>\n\nاتصال کاربران دوباره برقرار خواهد شد.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ بازگشت به پنل", callback_data="admin_panel")]]),
        parse_mode="HTML"
    )

@dp.callback_query(F.data == "do_restart_panel")
async def do_restart_panel(callback: types.CallbackQuery):
    if callback.from_user.id not in get_admin_ids(): return
    xui = XUIClient()
    await callback.message.edit_text(
        "🔥 <b>در حال ری‌استارت کامل پنل...</b>\n\n"
        "پنل در حال راه‌اندازی مجدد است. تا چند لحظه دیگر در دسترس خواهد بود.\n"
        "در صورت بروز خطا، چند دقیقه دیگر تلاش کنید.",
        parse_mode="HTML"
    )
    await xui.restart_panel()
    await callback.message.edit_text(
        "⚠️ <b>دستور ری‌استارت پنل صادر شد.</b>\n\n"
        "پنل در حال راه‌اندازی مجدد است. در صورت بروز مشکل، از راه دور سرور را بررسی کنید.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ بازگشت به پنل", callback_data="admin_panel")]]),
        parse_mode="HTML"
    )

@dp.callback_query(F.data == "admin_manage_user")
async def admin_ask_user(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in get_admin_ids(): return
    await callback.message.edit_text("👤 <b>لطفاً نام سرویس (ایمیل کلاینت) را دقیق تایپ کنید:</b>", reply_markup=get_cancel_kb(), parse_mode="HTML")
    await state.set_state(AdminFlow.wait_for_manage_email)

@dp.message(AdminFlow.wait_for_manage_email)
async def admin_fetch_user(message: types.Message, state: FSMContext):
    email = message.text.strip()
    xui = XUIClient()
    full_client_response = await xui.get_client_full(email)
    if not full_client_response or 'client' not in full_client_response:
        return await message.answer("❌ کلاینتی با این نام یافت نشد. دوباره امتحان کنید.", reply_markup=get_cancel_kb())
    client_data = full_client_response['client']
    is_enabled = client_data.get('enable', False)
    used_gb = full_client_response.get('usedTraffic', 0) / (1024 ** 3)
    text = f"""🔍 <b>مدیریت کلاینت:</b> <code>{email}</code>
━━━━━━━━━━━━━━━━━━
وضعیت اکانت: {'🟢 فعال' if is_enabled else '🔴 مسدود/غیرفعال'}
حجم مصرفی: {used_gb:.2f} GB
━━━━━━━━━━━━━━━━━━"""
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔴 مسدود کردن (Ban)" if is_enabled else "🟢 رفع مسدودیت (Unban)", callback_data=f"admtoggle_{email}")],
        [InlineKeyboardButton(text="🗑 حذف کامل کلاینت", callback_data=f"admdel_{email}")],
        [InlineKeyboardButton(text="🧹 آزادسازی آی‌پی‌ها", callback_data=f"admclearip_{email}")],
        [InlineKeyboardButton(text="⬅️ بازگشت به منو", callback_data="admin_panel")]
    ])
    await state.clear()
    await message.answer(text, reply_markup=kb, parse_mode="HTML")

@dp.callback_query(F.data.startswith("admtoggle_"))
async def adm_toggle_user(callback: types.CallbackQuery):
    if callback.from_user.id not in get_admin_ids(): return
    email = callback.data.split("_", 1)[1]
    xui = XUIClient()
    full = await xui.get_client_full(email)
    client_data = full['client']
    client_data['enable'] = not client_data['enable']
    await xui.update_client(email, client_data)
    await callback.answer(f"✅ وضعیت کلاینت به {'فعال' if client_data['enable'] else 'مسدود'} تغییر یافت.", show_alert=True)
    await callback.message.delete()

@dp.callback_query(F.data.startswith("admdel_"))
async def adm_delete_user(callback: types.CallbackQuery):
    if callback.from_user.id not in get_admin_ids(): return
    email = callback.data.split("_", 1)[1]
    xui = XUIClient()
    await xui.delete_client(email)
    await callback.answer(f"✅ کلاینت {email} به طور کامل از پنل حذف شد.", show_alert=True)
    await callback.message.delete()

@dp.callback_query(F.data.startswith("admclearip_"))
async def adm_clearip_user(callback: types.CallbackQuery):
    if callback.from_user.id not in get_admin_ids(): return
    email = callback.data.split("_", 1)[1]
    xui = XUIClient()
    await xui.clear_client_ips(email)
    await callback.answer("✅ قفل آی‌پی‌های کاربر شکسته شد.", show_alert=True)

# ==============================================================================
# RECONCILE INVISIBLE SERVICES (repair client_name -> real panel email)
# ==============================================================================
# Some services became invisible in "My Services" because the invoice's
# client_name was stored as the bare typed name instead of the real panel email
# ({name}_{invoice_id}). These handlers repair that mismatch (name repair only).

@dp.callback_query(F.data == "admin_reconcile_names")
async def admin_reconcile_names(callback: types.CallbackQuery):
    """Kick off a full-panel scan on the worker. It's a multi-minute job
    (worker concurrency=1), so we dispatch to Celery and let it DM the admin a
    dry-run summary with a tap-to-apply button — never block the bot loop."""
    if callback.from_user.id not in get_admin_ids():
        return await callback.answer("⛔ دسترسی غیرمجاز", show_alert=True)
    try:
        tasks.reconcile_client_names.delay(callback.from_user.id)
        await callback.message.edit_text(
            "🔧 <b>بازسازی نام سرویس‌ها آغاز شد</b>\n"
            "━━━━━━━━━━━━━━━━━━\n"
            "کل پنل در حال بررسی است؛ این کار چند دقیقه طول می‌کشد.\n"
            "گزارش به‌همراه دکمهٔ «اعمال» به‌صورت پیام برای شما ارسال می‌شود.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⬅️ بازگشت به پنل", callback_data="admin_panel")]
            ]),
            parse_mode="HTML"
        )
    except Exception as e:
        await callback.message.edit_text(
            f"❌ <b>خطا در صدور دستور:</b>\n<code>{str(e)[:200]}</code>",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ بازگشت", callback_data="admin_panel")]]),
            parse_mode="HTML"
        )

@dp.callback_query(F.data == "reconcile_apply")
async def reconcile_apply(callback: types.CallbackQuery):
    """Apply the plan the full-scan task stashed in Redis. Applying is just DB
    writes + cache clears (no panel calls), so it runs inline here — no second
    multi-minute scan."""
    if callback.from_user.id not in get_admin_ids():
        return await callback.answer("⛔ دسترسی غیرمجاز", show_alert=True)
    key = f"reconcile_plan:{callback.from_user.id}"
    raw = await redis_client.get(key)
    if not raw:
        return await callback.answer("⛔ برنامهٔ تعمیر منقضی شده است. دوباره اسکن کنید.", show_alert=True)
    try:
        plan = json.loads(raw)
    except Exception:
        plan = []
    await callback.answer("در حال اعمال تغییرات...")
    n = await apply_plan(plan)
    await redis_client.delete(key)
    await callback.message.edit_text(
        "✅ <b>تعمیر انجام شد</b>\n"
        "━━━━━━━━━━━━━━━━━━\n"
        f"🛠 <b>{n}</b> سرویس بازسازی شد و اکنون در «سرویس‌های من» نمایش داده می‌شود.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ بازگشت به پنل", callback_data="admin_panel")]
        ]),
        parse_mode="HTML"
    )

@dp.callback_query(F.data == "admin_reconcile_user")
async def admin_reconcile_user_ask(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in get_admin_ids():
        return await callback.answer("⛔ دسترسی غیرمجاز", show_alert=True)
    await callback.message.edit_text(
        "🔧 <b>تعمیر سرویس‌های یک کاربر</b>\n"
        "━━━━━━━━━━━━━━━━━━\n"
        "آی‌دی عددی تلگرام کاربر (یا آی‌دی نمایندگی) را ارسال کنید:",
        reply_markup=get_cancel_kb(), parse_mode="HTML"
    )
    await state.set_state(AdminFlow.wait_for_reconcile_user)

@dp.message(AdminFlow.wait_for_reconcile_user)
async def admin_reconcile_user_run(message: types.Message, state: FSMContext):
    """Repair one user's invisible services. Scoped to a single panel group, so
    it's fast enough to compute inline (unlike the full scan)."""
    gid = (message.text or "").strip()
    if not gid.isdigit():
        return await message.answer("❌ لطفاً یک آی‌دی عددی معتبر ارسال کنید.", reply_markup=get_cancel_kb())
    await state.clear()
    status = await message.answer("⏳ در حال بررسی سرویس‌های کاربر...")
    result = await compute_reconcile(get_xui_client(), gid)
    fixes = result["fixes"]
    back_kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ بازگشت به پنل", callback_data="admin_panel")]
    ])
    if not fixes:
        return await status.edit_text(
            "🔧 <b>تعمیر سرویس‌های کاربر</b>\n"
            "━━━━━━━━━━━━━━━━━━\n"
            f"👤 کاربر: <code>{gid}</code>\n"
            f"🔎 بررسی‌شده: <b>{len(result['records'])}</b> فاکتور\n"
            f"❓ مبهم: <b>{len(result['ambiguous'])}</b> | 🚫 روی پنل نیست: <b>{len(result['not_found'])}</b>\n\n"
            "✅ موردی برای تعمیر یافت نشد.",
            reply_markup=back_kb, parse_mode="HTML"
        )
    n = await apply_plan(to_plan(fixes))
    await status.edit_text(
        "✅ <b>تعمیر انجام شد</b>\n"
        "━━━━━━━━━━━━━━━━━━\n"
        f"👤 کاربر: <code>{gid}</code>\n"
        f"🛠 <b>{n}</b> سرویس بازسازی شد و اکنون در «سرویس‌های من» نمایش داده می‌شود.\n"
        f"❓ مبهم: <b>{len(result['ambiguous'])}</b> | 🚫 روی پنل نیست: <b>{len(result['not_found'])}</b>",
        reply_markup=back_kb, parse_mode="HTML"
    )

# ============================================================================
# IMPORT EXISTING PANEL GROUP CONFIGS
# ============================================================================
SYNC_PAGE_SIZE = 8
SYNC_TTL = 900

def _sync_key(admin_id: int, token: str) -> str:
    return f"panel_sync:{admin_id}:{token}"

def _sync_list_kb(token: str, items: list, selected: set, page: int = 0):
    start = page * SYNC_PAGE_SIZE
    page_items = items[start:start + SYNC_PAGE_SIZE]
    rows = []
    for idx, email in enumerate(page_items, start):
        mark = "✅" if idx in selected else "⬜"
        rows.append([InlineKeyboardButton(text=f"{mark} {email[:45]}", callback_data=f"sync_t:{token}:{idx}:{page}")])
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"sync_p:{token}:{page - 1}"))
    if start + SYNC_PAGE_SIZE < len(items):
        nav.append(InlineKeyboardButton(text="➡️", callback_data=f"sync_p:{token}:{page + 1}"))
    if nav:
        rows.append(nav)
    rows.extend([
        [InlineKeyboardButton(text="✅ انتخاب همه", callback_data=f"sync_all:{token}"), InlineKeyboardButton(text="🧹 پاک کردن", callback_data=f"sync_none:{token}")],
        [InlineKeyboardButton(text="📥 ادامه و انتخاب پلن", callback_data=f"sync_go:{token}")],
        [InlineKeyboardButton(text="❌ انصراف", callback_data="cancel")],
    ])
    return InlineKeyboardMarkup(inline_keyboard=rows)

async def _render_sync_list(message, token: str, data: dict, page: int = 0):
    items = data.get("items", [])
    selected = set(data.get("selected", []))
    await message.edit_text(
        "📥 <b>همگام‌سازی گروه پنل</b>\n"
        "━━━━━━━━━━━━━━━━━━\n"
        f"👤 گروه/کاربر: <code>{data['group_id']}</code>\n"
        f"🔎 موارد قابل همگام‌سازی: <b>{len(items)}</b>\n"
        f"✅ انتخاب‌شده: <b>{len(selected)}</b>\n\n"
        "فقط سرویس‌هایی را انتخاب کنید که باید در ربات ثبت شوند.",
        reply_markup=_sync_list_kb(token, items, selected, page), parse_mode="HTML"
    )

@dp.callback_query(F.data == "admin_sync_group")
async def admin_sync_group_start(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in get_admin_ids():
        return await callback.answer("⛔ دسترسی غیرمجاز", show_alert=True)
    await state.set_state(AdminFlow.wait_for_sync_group)
    await callback.message.edit_text(
        "📥 <b>همگام‌سازی گروه پنل</b>\n\nنام گروه همان شناسه عددی تلگرام کاربر است. ارسال کنید:",
        reply_markup=get_cancel_kb(), parse_mode="HTML"
    )

@dp.message(AdminFlow.wait_for_sync_group)
async def admin_sync_group_scan(message: types.Message, state: FSMContext):
    if message.from_user.id not in get_admin_ids():
        await state.clear()
        return
    raw = (message.text or "").strip()
    if not raw.isdigit():
        return await message.answer("❌ شناسه باید فقط عدد باشد.", reply_markup=get_cancel_kb())
    group_id = int(raw)
    if is_reseller(group_id):
        return await message.answer("❌ همگام‌سازی نمایندگان در این نسخه پشتیبانی نمی‌شود.", reply_markup=get_cancel_kb())
    try:
        panel_emails = list(dict.fromkeys(await get_xui_client().get_group_emails(str(group_id))))
    except Exception as exc:
        return await message.answer(f"❌ دریافت گروه از پنل ناموفق بود: <code>{str(exc)[:180]}</code>", reply_markup=get_cancel_kb(), parse_mode="HTML")
    with SessionLocal() as db:
        known = {row[0] for row in db.query(Invoice.client_name).filter(Invoice.telegram_user_id == group_id, Invoice.client_name.isnot(None)).all()}
        globally_owned = {row[0] for row in db.query(Invoice.client_name).filter(Invoice.client_name.in_(panel_emails), Invoice.telegram_user_id != group_id).all()}
    items = [email for email in panel_emails if email not in known and email not in globally_owned]
    conflicts = len([email for email in panel_emails if email in globally_owned])
    if not items:
        await state.clear()
        return await message.answer(f"✅ مورد جدیدی برای گروه <code>{group_id}</code> پیدا نشد.\n⚠️ تعارض‌ها: {conflicts}", reply_markup=get_back_kb("admin_panel"), parse_mode="HTML")
    token = secrets.token_urlsafe(6)
    await redis_client.set(_sync_key(message.from_user.id, token), json.dumps({"group_id": group_id, "items": items, "selected": [], "plans": {}, "conflicts": conflicts}), ex=SYNC_TTL)
    await state.clear()
    status = await message.answer("⏳ در حال آماده‌سازی فهرست...")
    await _render_sync_list(status, token, {"group_id": group_id, "items": items, "selected": [], "plans": {}, "conflicts": conflicts})

async def _load_sync(admin_id, token):
    raw = await redis_client.get(_sync_key(admin_id, token))
    return json.loads(raw) if raw else None

@dp.callback_query(F.data.startswith("sync_t:"))
async def admin_sync_toggle(callback: types.CallbackQuery):
    if callback.from_user.id not in get_admin_ids(): return
    _, token, raw_idx, raw_page = callback.data.split(":")
    data = await _load_sync(callback.from_user.id, token)
    if not data: return await callback.answer("⛔ نشست منقضی شده است.", show_alert=True)
    idx = int(raw_idx); selected = set(data["selected"])
    if idx in selected: selected.remove(idx)
    else: selected.add(idx)
    data["selected"] = sorted(selected)
    await redis_client.set(_sync_key(callback.from_user.id, token), json.dumps(data), ex=SYNC_TTL)
    await callback.answer()
    await _render_sync_list(callback.message, token, data, int(raw_page))

@dp.callback_query(F.data.startswith("sync_all:"))
async def admin_sync_select_all(callback: types.CallbackQuery):
    if callback.from_user.id not in get_admin_ids(): return
    token = callback.data.split(":", 1)[1]; data = await _load_sync(callback.from_user.id, token)
    if not data: return await callback.answer("⛔ نشست منقضی شده است.", show_alert=True)
    data["selected"] = list(range(len(data["items"])))
    await redis_client.set(_sync_key(callback.from_user.id, token), json.dumps(data), ex=SYNC_TTL)
    await callback.answer()
    await _render_sync_list(callback.message, token, data)

@dp.callback_query(F.data.startswith("sync_none:"))
async def admin_sync_select_none(callback: types.CallbackQuery):
    if callback.from_user.id not in get_admin_ids(): return
    token = callback.data.split(":", 1)[1]; data = await _load_sync(callback.from_user.id, token)
    if not data: return await callback.answer("⛔ نشست منقضی شده است.", show_alert=True)
    data["selected"] = []
    await redis_client.set(_sync_key(callback.from_user.id, token), json.dumps(data), ex=SYNC_TTL)
    await callback.answer()
    await _render_sync_list(callback.message, token, data)

@dp.callback_query(F.data.startswith("sync_p:"))
async def admin_sync_page(callback: types.CallbackQuery):
    if callback.from_user.id not in get_admin_ids(): return
    _, token, raw_page = callback.data.split(":"); data = await _load_sync(callback.from_user.id, token)
    if not data: return await callback.answer("⛔ نشست منقضی شده است.", show_alert=True)
    await callback.answer()
    await _render_sync_list(callback.message, token, data, int(raw_page))

@dp.callback_query(F.data.startswith("sync_go:"))
async def admin_sync_choose_plan(callback: types.CallbackQuery):
    if callback.from_user.id not in get_admin_ids(): return
    token = callback.data.split(":", 1)[1]; data = await _load_sync(callback.from_user.id, token)
    if not data: return await callback.answer("⛔ نشست منقضی شده است.", show_alert=True)
    selected = data.get("selected", [])
    if not selected: return await callback.answer("⚠️ حداقل یک سرویس را انتخاب کنید.", show_alert=True)
    with SessionLocal() as db:
        plans = db.query(Plan).filter(Plan.is_active == True).order_by(Plan.id.asc()).all()
        plan_rows = [(p.id, p.name, p.traffic_gb, p.duration_days, p.price) for p in plans]
    if not plan_rows: return await callback.answer("❌ هیچ پلن فعالی وجود ندارد.", show_alert=True)
    data["plan_rows"] = plan_rows; data["plan_pos"] = 0
    await redis_client.set(_sync_key(callback.from_user.id, token), json.dumps(data), ex=SYNC_TTL)
    email = data["items"][selected[0]]
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=f"📦 {n} | {gb}GB / {days}روز | {price:,} تومان", callback_data=f"sync_plan:{token}:{pid}")] for pid,n,gb,days,price in plan_rows] + [[InlineKeyboardButton(text="❌ انصراف", callback_data="cancel")]])
    await callback.message.edit_text(f"📦 <b>انتخاب پلن (1 از {len(selected)})</b>\n\nسرویس: <code>{email}</code>\nبرای هر سرویس یک پلن انتخاب کنید.", reply_markup=kb, parse_mode="HTML")

@dp.callback_query(F.data.startswith("sync_plan:"))
async def admin_sync_plan_selected(callback: types.CallbackQuery):
    if callback.from_user.id not in get_admin_ids(): return
    _, token, raw_plan = callback.data.split(":"); data = await _load_sync(callback.from_user.id, token)
    if not data: return await callback.answer("⛔ نشست منقضی شده است.", show_alert=True)
    selected = data["selected"]; pos = int(data.get("plan_pos", 0)); data.setdefault("plans", {})[str(selected[pos])] = int(raw_plan)
    pos += 1; data["plan_pos"] = pos
    if pos < len(selected):
        await redis_client.set(_sync_key(callback.from_user.id, token), json.dumps(data), ex=SYNC_TTL)
        email = data["items"][selected[pos]]; rows = data["plan_rows"]
        kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=f"📦 {n} | {gb}GB / {days}روز | {price:,} تومان", callback_data=f"sync_plan:{token}:{pid}")] for pid,n,gb,days,price in rows] + [[InlineKeyboardButton(text="❌ انصراف", callback_data="cancel")]])
        return await callback.message.edit_text(f"📦 <b>انتخاب پلن ({pos + 1} از {len(selected)})</b>\n\nسرویس: <code>{email}</code>", reply_markup=kb, parse_mode="HTML")
    await redis_client.set(_sync_key(callback.from_user.id, token), json.dumps(data), ex=SYNC_TTL)
    plan_names = {str(pid): name for pid, name, _, _, _ in data["plan_rows"]}
    summary_lines = [f"• <code>{data['items'][i]}</code> → {plan_names.get(str(data['plans'][str(i)]), 'پلن نامشخص')}" for i in selected[:20]]
    if len(selected) > 20:
        summary_lines.append(f"... و {len(selected) - 20} سرویس دیگر")
    summary = "\n".join(summary_lines)
    await callback.message.edit_text(f"🔎 <b>تأیید همگام‌سازی</b>\n━━━━━━━━━━━━━━━━━━\n{summary}\n\nهیچ تغییری در پنل انجام نمی‌شود.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="✅ تأیید", callback_data=f"sync_confirm:{token}")],[InlineKeyboardButton(text="❌ انصراف", callback_data="cancel")]]), parse_mode="HTML")

@dp.callback_query(F.data.startswith("sync_confirm:"))
async def admin_sync_confirm(callback: types.CallbackQuery):
    if callback.from_user.id not in get_admin_ids(): return
    token = callback.data.split(":", 1)[1]; data = await _load_sync(callback.from_user.id, token)
    if not data: return await callback.answer("⛔ نشست منقضی شده است.", show_alert=True)
    try:
        current = set(await get_xui_client().get_group_emails(str(data["group_id"])))
        imported = skipped = conflicts = 0
        with SessionLocal() as db:
            for idx in data["selected"]:
                email = data["items"][idx]
                if email not in current:
                    skipped += 1; continue
                if db.query(Invoice).filter(Invoice.client_name == email, Invoice.telegram_user_id != data["group_id"]).first():
                    conflicts += 1; continue
                if db.query(Invoice).filter(Invoice.client_name == email, Invoice.telegram_user_id == data["group_id"]).first():
                    skipped += 1; continue
                plan_id = data["plans"].get(str(idx))
                if not plan_id or not db.query(Plan).filter(Plan.id == plan_id, Plan.is_active == True).first():
                    conflicts += 1; continue
                db.add(Invoice(telegram_user_id=data["group_id"], plan_id=plan_id, total_price=0, original_price=0, discount_amount=0, client_name=email, action_type="PANEL_SYNC", status="COMPLETE", description="Imported from existing panel group"))
                imported += 1
            db.commit()
        await invalidate_user_service_cache(data["group_id"])
        await redis_client.delete(_sync_key(callback.from_user.id, token))
        await callback.message.edit_text(f"✅ <b>همگام‌سازی انجام شد</b>\n\n📥 ثبت‌شده: <b>{imported}</b>\n⏭ ردشده/قبلی: <b>{skipped}</b>\n⚠️ تعارض یا پلن نامعتبر: <b>{conflicts}</b>", reply_markup=get_back_kb("admin_panel"), parse_mode="HTML")
    except Exception as exc:
        logger.exception("Panel group sync failed")
        await callback.answer(f"❌ خطا در همگام‌سازی: {str(exc)[:180]}", show_alert=True)

# ==============================================================================
# INVOICE APPROVAL
# ==============================================================================
@dp.callback_query(F.data.startswith("reject_"))
async def admin_reject_start(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in get_admin_ids(): return
    invoice_id = int(callback.data.split("_")[1])
    with SessionLocal() as db:
        # Atomic lock: only succeed if still PENDING
        rows = db.query(Invoice).filter(
            Invoice.id == invoice_id,
            Invoice.status == "PENDING"
        ).update({"status": "LOCKED"})
        db.commit()
        if rows == 0:
            invoice = db.query(Invoice).filter(Invoice.id == invoice_id).first()
            status_msg = invoice.status if invoice else "نامشخص"
            await callback.answer(f"⚠️ از قبل پردازش شده است! وضعیت فعلی: {status_msg}", show_alert=True)
            try: await callback.message.edit_reply_markup(reply_markup=None)
            except Exception as e: logger.debug(f"Failed to remove keyboard: {e}")
            return
    try: await callback.message.edit_reply_markup(reply_markup=None)
    except Exception as e: logger.debug(f"Failed to remove keyboard: {e}")
    await state.update_data(invoice_id=invoice_id)
    await callback.message.reply(f"⛔️ <b>رد کردن فاکتور #{invoice_id}</b>\n\nلطفاً دلیل رد فاکتور را برای کاربر بنویسید:", reply_markup=get_cancel_kb(), parse_mode="HTML")
    await state.set_state(AdminFlow.wait_for_reject_reason)

@dp.message(AdminFlow.wait_for_reject_reason)
async def admin_reject_reason(message: types.Message, state: FSMContext):
    data = await state.get_data()
    invoice_id = data['invoice_id']
    with SessionLocal() as db:
        invoice = db.query(Invoice).filter(Invoice.id == invoice_id).first()
        invoice.status = "REJECTED"
        db.commit()
        # Capture before the session closes (commit expires the instance)
        target_user_id = invoice.telegram_user_id
    await bot.send_message(target_user_id, f"❌ <b>متاسفانه فاکتور شما رد شد.</b>\n\n<b>دلیل:</b> <i>{message.text}</i>", parse_mode="HTML")
    await message.answer(f"✅ فاکتور #{invoice_id} در سیستم رد شد.")
    await state.clear()

@dp.callback_query(F.data.startswith("approve_"))
async def admin_approve_new(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in get_admin_ids(): return
    invoice_id = int(callback.data.split("_")[1])
    with SessionLocal() as db:
        # Atomic lock: only succeed if still PENDING
        rows = db.query(Invoice).filter(
            Invoice.id == invoice_id,
            Invoice.status == "PENDING"
        ).update({"status": "LOCKED"})
        db.commit()
        if rows == 0:
            invoice = db.query(Invoice).filter(Invoice.id == invoice_id).first()
            status_msg = invoice.status if invoice else "نامشخص"
            await callback.answer(f"⚠️ از قبل پردازش شده است! وضعیت فعلی: {status_msg}", show_alert=True)
            try: await callback.message.edit_reply_markup(reply_markup=None)
            except Exception as e: logger.debug(f"Failed to remove keyboard: {e}")
            return
        invoice = db.query(Invoice).filter(Invoice.id == invoice_id).first()
    try: await callback.message.edit_reply_markup(reply_markup=None)
    except Exception as e: logger.debug(f"Failed to remove keyboard: {e}")
    if invoice.action_type == "RENEW":
        await callback.message.edit_caption(caption=f"🔄 <b>در حال تمدید سرویس...</b>\n\nوظیفه به صف پس‌زمینه برای فاکتور #{invoice_id} ارسال شد.", reply_markup=None, parse_mode="HTML")
        await redis_client.set(f"loading:{invoice_id}", f"{callback.message.chat.id}:{callback.message.message_id}", ex=300)
        tasks.provision_renew.delay(invoice.id, invoice.client_name)
        await invalidate_user_service_cache(invoice.telegram_user_id)
        with SessionLocal() as db:
            inv = db.query(Invoice).filter(Invoice.id == invoice_id).first()
            inv.status = "PROCESSING"
            db.commit()
        await bot.send_message(invoice.telegram_user_id, "🎉 <b>پرداخت شما تایید شد!</b>\nسرویس شما در حال تمدید است.\n🔗 لینک اتصال پس از تکمیل آماده‌سازی برای شما ارسال خواهد شد.", parse_mode="HTML")
        return
    elif invoice.action_type == "TOPUP":
        await callback.message.edit_caption(caption=f"➕ <b>در حال افزودن حجم...</b>\n\nوظیفه به صف پس‌زمینه برای فاکتور #{invoice_id} ارسال شد.", reply_markup=None, parse_mode="HTML")
        await redis_client.set(f"loading:{invoice_id}", f"{callback.message.chat.id}:{callback.message.message_id}", ex=300)
        tasks.provision_topup.delay(invoice.id, invoice.client_name)
        await invalidate_user_service_cache(invoice.telegram_user_id)
        with SessionLocal() as db:
            inv = db.query(Invoice).filter(Invoice.id == invoice_id).first()
            inv.status = "PROCESSING"
            db.commit()
        await bot.send_message(invoice.telegram_user_id, "🎉 <b>پرداخت شما تایید شد!</b>\nحجم خریداری شده در حال اضافه شدن است.\n🔗 لینک اتصال پس از تکمیل آماده‌سازی برای شما ارسال خواهد شد.", parse_mode="HTML")
        return
    elif invoice.action_type == "CUSTOM_ORDER":
        # Mark as COMPLETE
        with SessionLocal() as db:
            inv = db.query(Invoice).filter(Invoice.id == invoice_id).first()
            if inv:
                inv.status = "COMPLETE"
                db.commit()
        await callback.message.edit_caption(caption=f"✅ <b>فاکتور سفارشی تایید شد.</b>\n\nفاکتور #{invoice_id} تایید شد.", reply_markup=None, parse_mode="HTML")
        await bot.send_message(invoice.telegram_user_id, "🎉 <b>فاکتور سفارشی شما تایید شد!</b>\n\nپرداخت شما ثبت گردید. با تشکر.", parse_mode="HTML")
        return
    elif invoice.action_type == "RESELLER_PACK_BUY":
        # Call the Celery task to add the pack
        await callback.message.edit_caption(caption=f"📦 <b>در حال افزودن بسته ترافیک...</b>\n\nوظیفه به صف پس‌زمینه برای فاکتور #{invoice_id} ارسال شد.", reply_markup=None, parse_mode="HTML")
        tasks.add_reseller_pack.delay(invoice_id)
        with SessionLocal() as db:
            inv = db.query(Invoice).filter(Invoice.id == invoice_id).first()
            if inv:
                inv.status = "PROCESSING"
                db.commit()
        await bot.send_message(invoice.telegram_user_id, "🎉 <b>پرداخت شما تایید شد!</b>\nبسته ترافیک شما در حال اضافه شدن است.\nپس از تکمیل، به شما اعلام خواهد شد.", parse_mode="HTML")
        return
    elif invoice.action_type and invoice.action_type.startswith("AMNEZIA_"):
        kind = invoice.action_type.split("_", 1)[1]  # NEW / RENEW / TOPUP
        kind_captions = {
            "NEW": "🟣 <b>در حال ساخت سرویس Amnezia...</b>",
            "RENEW": "🔄 <b>در حال تمدید سرویس Amnezia...</b>",
            "TOPUP": "➕ <b>در حال افزودن حجم Amnezia...</b>",
        }
        await callback.message.edit_caption(
            caption=(f"{kind_captions.get(kind, '🟣 <b>در حال پردازش...</b>')}\n\n"
                     f"وظیفه به صف پس‌زمینه برای فاکتور #{invoice_id} ارسال شد."),
            reply_markup=None, parse_mode="HTML")
        await redis_client.set(f"loading:{invoice_id}", f"{callback.message.chat.id}:{callback.message.message_id}", ex=300)
        task_map = {
            "NEW": tasks.provision_amnezia_new,
            "RENEW": tasks.provision_amnezia_renew,
            "TOPUP": tasks.provision_amnezia_topup,
        }
        task_map[kind].delay(invoice_id)
        with SessionLocal() as db:
            inv = db.query(Invoice).filter(Invoice.id == invoice_id).first()
            inv.status = "PROCESSING"
            db.commit()
        kind_user_msgs = {
            "NEW": "🎉 <b>پرداخت شما تایید شد!</b>\nسرویس Amnezia شما در حال ساخت است.\n🔗 لینک و کانفیگ پس از تکمیل برای شما ارسال خواهد شد.",
            "RENEW": "🎉 <b>پرداخت شما تایید شد!</b>\nسرویس Amnezia شما در حال تمدید است.",
            "TOPUP": "🎉 <b>پرداخت شما تایید شد!</b>\nحجم خریداری شده در حال اضافه شدن است.",
        }
        await bot.send_message(invoice.telegram_user_id, kind_user_msgs.get(kind, kind_user_msgs["NEW"]), parse_mode="HTML")
        return
    xui = XUIClient()
    try:
        inbounds = await xui.get_enabled_inbounds()
    except Exception as e:
        # Release the admin claim if panel discovery fails before the job is queued.
        with SessionLocal() as db:
            db.query(Invoice).filter(
                Invoice.id == invoice_id,
                Invoice.status == "LOCKED"
            ).update({"status": "PENDING"})
            db.commit()
        await callback.message.reply(
            f"❌ <b>خطا در ارتباط با پنل</b>\n\n"
            f"پنل شما به آدرس <code>{xui.base_url}</code> در دسترس نیست یا API نامعتبر است.\n\n"
            f"<b>خطا:</b> <code>{e}</code>\n\n"
            f"📌 <b>نکته:</b> این ربات با پنل‌های سازگار با API 3x-ui کار می‌کند.\n"
            f"لطفاً از صحت URL پنل و مسیر API آن مطمئن شوید.",
            parse_mode="HTML"
        )
        return
    await state.update_data(invoice_id=invoice_id, selected_inbounds=[], inbounds_list=inbounds)
    await render_inbound_selection_kb(callback.message, state, inbounds, set())
    await state.set_state(AdminFlow.wait_for_inbounds)

async def render_inbound_selection_kb(message: types.Message, state: FSMContext, inbounds: list, selected: set):
    """Render the inbound selection keyboard with select-all toggle."""
    # Build per-inbound buttons
    kb_buttons = []
    for ib in inbounds:
        mark = '🟢' if ib['id'] in selected else '🔘'
        kb_buttons.append([InlineKeyboardButton(
            text=f"{mark} {ib['remark']} (Port {ib['port']})",
            callback_data=f"toggle_ib_{ib['id']}"
        )])

    # Select-all toggle button (only if there are inbounds)
    if inbounds:
        all_selected = len(selected) == len(inbounds)
        toggle_text = "📌 لغو انتخاب همه" if all_selected else "📌 انتخاب همه"
        kb_buttons.append([InlineKeyboardButton(text=toggle_text, callback_data="toggle_all_inbounds")])

    # Confirm and cancel buttons
    kb_buttons.append([InlineKeyboardButton(text="✅ تایید نهایی و ساخت سرویس", callback_data="confirm_provision", style="success")])
    kb_buttons.append([InlineKeyboardButton(text="❌ انصراف", callback_data="cancel")])

    kb = InlineKeyboardMarkup(inline_keyboard=kb_buttons)
    try:
        await message.edit_caption(
            caption="✅ <b>فاکتور تایید شد.</b>\n\n"
            "سرور(های) مورد نظر برای این کاربر جدید را انتخاب کنید:",
            reply_markup=kb,
            parse_mode="HTML"
        )
    except Exception:
        # If editing fails (e.g., message type mismatch), send a new message
        await message.delete()
        await message.answer(
            "✅ <b>فاکتور تایید شد.</b>\n\n"
            "سرور(های) مورد نظر برای این کاربر جدید را انتخاب کنید:",
            reply_markup=kb,
            parse_mode="HTML"
        )

@dp.callback_query(AdminFlow.wait_for_inbounds, F.data.startswith("toggle_ib_"))
async def toggle_inbound(callback: types.CallbackQuery, state: FSMContext):
    ib_id = int(callback.data.split("_")[2])
    data = await state.get_data()
    selected = set(data.get('selected_inbounds', []))
    inbounds = data.get('inbounds_list', [])
    if ib_id in selected:
        selected.remove(ib_id)
    else:
        selected.add(ib_id)
    await state.update_data(selected_inbounds=list(selected))
    await render_inbound_selection_kb(callback.message, state, inbounds, selected)

@dp.callback_query(AdminFlow.wait_for_inbounds, F.data == "toggle_all_inbounds")
async def toggle_all_inbounds(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    inbounds = data.get('inbounds_list', [])
    selected = set(data.get('selected_inbounds', []))
    if len(selected) == len(inbounds) and inbounds:
        # Deselect all
        selected.clear()
    else:
        # Select all
        selected = {ib['id'] for ib in inbounds}
    await state.update_data(selected_inbounds=list(selected))
    await render_inbound_selection_kb(callback.message, state, inbounds, selected)

@dp.callback_query(AdminFlow.wait_for_inbounds, F.data == "confirm_provision")
async def execute_provision(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if not data.get('selected_inbounds'): return await callback.answer("حداقل باید ۱ سرور انتخاب کنید!", show_alert=True)
    with SessionLocal() as db:
        # Atomic: queue work only once for the admin-claimed invoice.
        rows = db.query(Invoice).filter(
            Invoice.id == data['invoice_id'],
            Invoice.status == "LOCKED"
        ).update({"status": "PROCESSING"})
        db.commit()
        if rows == 0:
            return await callback.answer("⚠️ این فاکتور قبلا تایید یا لغو شده است.", show_alert=True)
        invoice = db.query(Invoice).filter(Invoice.id == data['invoice_id']).first()
    await callback.message.edit_caption(caption=f"⚙️ <b>در حال ساخت سرویس...</b>\n\nوظیفه به صف پس‌زمینه برای فاکتور #{data['invoice_id']} ارسال شد.", reply_markup=None, parse_mode="HTML")
    await redis_client.set(f"loading:{data['invoice_id']}", f"{callback.message.chat.id}:{callback.message.message_id}", ex=300)
    tasks.provision_new.delay(data['invoice_id'], data['selected_inbounds'])
    await invalidate_user_service_cache(invoice.telegram_user_id)
    await bot.send_message(invoice.telegram_user_id, "🎉 <b>پرداخت شما تایید شد!</b>\n\nسرویس شما به صورت خودکار در حال آماده‌سازی است.\n🔗 لینک اتصال پس از تکمیل آماده‌سازی برای شما ارسال خواهد شد.", parse_mode="HTML")
    await state.clear()

# ==============================================================================
# ADMIN: SETTINGS (Topup, Card, Sub, Plans)
# ==============================================================================

@dp.callback_query(F.data == "admin_set_card")
async def set_card_start(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in get_admin_ids(): return
    msg = """💳 <b>تنظیم اطلاعات پرداخت</b>\n\nشماره کارت و نام دارنده را تایپ کنید. مثال:\n<code>💳 1234-5678-9012-3456\n👤 علی محمدی</code>"""
    await callback.message.edit_text(msg, parse_mode="HTML", reply_markup=get_cancel_kb())
    await state.set_state(AdminFlow.wait_for_card)

@dp.message(AdminFlow.wait_for_card)
async def set_card_save(message: types.Message, state: FSMContext):
    with SessionLocal() as db:
        setting = db.query(AppSetting).filter(AppSetting.key == "payment_card").first()
        if not setting: db.add(AppSetting(key="payment_card", value=message.text))
        else: setting.value = message.text
        db.commit()
    await state.clear()
    await message.answer("✅ <b>اطلاعات پرداخت با موفقیت ثبت شد!</b>", parse_mode="HTML", reply_markup=get_admin_menu())

@dp.callback_query(F.data == "admin_set_sub_link")
async def set_sub_link_start(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in get_admin_ids(): return
    msg = """🔗 <b>تنظیم لینک پایه ساب</b>\n\nمثال:\n<code>https://gg.mx11.ir:2096/sub/</code>"""
    await callback.message.edit_text(msg, parse_mode="HTML", reply_markup=get_cancel_kb())
    await state.set_state(AdminFlow.wait_for_sub_link)

@dp.message(AdminFlow.wait_for_sub_link)
async def set_sub_link_save(message: types.Message, state: FSMContext):
    with SessionLocal() as db:
        setting = db.query(AppSetting).filter(AppSetting.key == "sub_base_link").first()
        if not setting: db.add(AppSetting(key="sub_base_link", value=message.text))
        else: setting.value = message.text
        db.commit()
    await state.clear()
    await message.answer("✅ <b>لینک پایه ساب با موفقیت تنظیم شد!</b>", parse_mode="HTML", reply_markup=get_admin_menu())

@dp.callback_query(F.data == "admin_set_support")
async def set_support_start(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in get_admin_ids(): return
    with SessionLocal() as db:
        setting = db.query(AppSetting).filter(AppSetting.key == 'support_url').first()
    current = setting.value if setting else "https://t.me/your_support"
    msg = f"""👤 <b>تنظیم حساب پشتیبانی</b>

لینک یا آیدی اکانت پشتیبانی را وارد کنید.

<b>موارد قابل قبول:</b>
• لینک تلگرام: <code>https://t.me/username</code>
• آیدی عددی: <code>123456789</code>
• یوزرنیم: <code>@username</code>

<b>مقدار فعلی:</b> <code>{current}</code>"""
    await callback.message.edit_text(msg, parse_mode="HTML", reply_markup=get_cancel_kb())
    await state.set_state(AdminFlow.wait_for_support_account)

@dp.message(AdminFlow.wait_for_support_account)
async def set_support_save(message: types.Message, state: FSMContext):
    if message.from_user.id not in get_admin_ids(): return
    value = message.text.strip()
    # Auto-convert pure numeric IDs to a t.me link
    if value.isdigit():
        value = f"https://t.me/{value}"
    elif value.startswith('@'):
        value = f"https://t.me/{value[1:]}"
    elif not value.startswith('http'):
        value = f"https://t.me/{value}"
    with SessionLocal() as db:
        setting = db.query(AppSetting).filter(AppSetting.key == "support_url").first()
        if not setting:
            db.add(AppSetting(key="support_url", value=value))
        else:
            setting.value = value
        db.commit()
    await state.clear()
    await message.answer("✅ <b>حساب پشتیبانی با موفقیت تنظیم شد!</b>", parse_mode="HTML", reply_markup=get_admin_menu())

@dp.callback_query(F.data == "admin_add_plan")
async def add_plan_start(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in get_admin_ids(): return
    await callback.message.edit_text("🏷 <b>نام پلن جدید را وارد کنید:</b>\n<i>(مثال: '50 گیگ - 1 ماهه')</i>", parse_mode="HTML", reply_markup=get_cancel_kb())
    await state.set_state(AddPlanFlow.wait_for_name)

@dp.callback_query(F.data == "admin_add_plan_amz")
async def add_plan_amz_start(callback: types.CallbackQuery, state: FSMContext):
    """Same wizard as admin_add_plan but preset to the Amnezia service type
    (the type question at the end is skipped)."""
    if callback.from_user.id not in get_admin_ids(): return
    await state.update_data(service_type='amnezia')
    await callback.message.edit_text(
        "🟣 <b>نام پلن Amnezia جدید را وارد کنید:</b>\n"
        "<i>(مثال: 'نامحدود - 1 ماهه' — حجم ۰ به معنای نامحدود است)</i>",
        parse_mode="HTML", reply_markup=get_cancel_kb())
    await state.set_state(AddPlanFlow.wait_for_name)

@dp.message(AddPlanFlow.wait_for_name)
async def add_plan_name(message: types.Message, state: FSMContext):
    await state.update_data(name=message.text)
    await message.answer("📶 <b>حجم این پلن را به گیگابایت وارد کنید:</b>\n\n💡 عدد ۰ = <b>نامحدود</b> (برای پلن‌های Amnezia)", parse_mode="HTML", reply_markup=get_cancel_kb())
    await state.set_state(AddPlanFlow.wait_for_gb)

@dp.message(AddPlanFlow.wait_for_gb)
async def add_plan_gb(message: types.Message, state: FSMContext):
    if not message.text.isdigit(): return await message.answer("⚠️ فقط عدد وارد کنید.", reply_markup=get_cancel_kb())
    await state.update_data(gb=int(message.text))
    await message.answer("⏳ <b>مدت زمان پلن را به روز وارد کنید:</b>\n\n💡 حجم ۰ به معنای <b>نامحدود</b> است (مخصوص پلن‌های Amnezia).", parse_mode="HTML", reply_markup=get_cancel_kb())
    await state.set_state(AddPlanFlow.wait_for_days)

@dp.message(AddPlanFlow.wait_for_days)
async def add_plan_days(message: types.Message, state: FSMContext):
    if not message.text.isdigit(): return await message.answer("⚠️ فقط عدد وارد کنید.", reply_markup=get_cancel_kb())
    await state.update_data(days=int(message.text))
    await message.answer("💵 <b>قیمت پلن را به تومان وارد کنید:</b>", parse_mode="HTML", reply_markup=get_cancel_kb())
    await state.set_state(AddPlanFlow.wait_for_price)

@dp.message(AddPlanFlow.wait_for_price)
async def add_plan_price(message: types.Message, state: FSMContext):
    if not message.text.isdigit(): return await message.answer("⚠️ فقط عدد وارد کنید.", reply_markup=get_cancel_kb())
    await state.update_data(price=int(message.text))
    data = await state.get_data()
    if data.get('service_type') == 'amnezia':
        # Started from the dedicated 🟣 shortcut — type already chosen.
        with SessionLocal() as db:
            db.add(Plan(name=data['name'], traffic_gb=data['gb'], duration_days=data['days'],
                        price=data['price'], is_active=True, service_type='amnezia'))
            db.commit()
        await state.clear()
        return await message.answer(
            f"✅ <b>پلن Amnezia '{data['name']}' با موفقیت اضافه شد!</b>",
            parse_mode="HTML", reply_markup=get_admin_menu())
    await message.answer(
        "🖥 <b>این پلن برای کدام سرویس است؟</b>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⚙️ سرویس معمولی (XUI)", callback_data="pltype_xui"),
             InlineKeyboardButton(text="🟣 Amnezia", callback_data="pltype_amnezia")]],
        ), parse_mode="HTML")

@dp.callback_query(F.data.startswith("pltype_"))
async def add_plan_type_chosen(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in get_admin_ids(): return
    stype = callback.data.split("_")[1]
    data = await state.get_data()
    with SessionLocal() as db:
        db.add(Plan(name=data['name'], traffic_gb=data['gb'], duration_days=data['days'],
                    price=data['price'], is_active=True,
                    service_type=stype if stype == 'amnezia' else 'xui'))
        db.commit()
    await state.clear()
    label = "🟣 Amnezia" if stype == 'amnezia' else "⚙️ XUI"
    await callback.message.edit_text(f"✅ <b>پلن '{data['name']}' ({label}) با موفقیت اضافه شد!</b>", parse_mode="HTML", reply_markup=get_admin_menu())

@dp.callback_query(F.data == "admin_view_plans")
async def view_active_plans(callback: types.CallbackQuery):
    if callback.from_user.id not in get_admin_ids(): return
    with SessionLocal() as db:
        plans = db.query(Plan).filter(Plan.is_active == True).all()
    if not plans: 
        return await callback.message.edit_text(
            "📋 <b>لیست پلن‌ها</b>\n\n❌ هیچ پلن فعالی یافت نشد.\nبرای افزودن پلن جدید از دکمه «➕ افزودن پلن» استفاده کنید.",
            reply_markup=get_admin_menu(), parse_mode="HTML"
        )
    text = "📋 <b>مدیریت پلن‌های فعال</b>\n━━━━━━━━━━━━━━━━━━\n"
    for i, p in enumerate(plans, 1):
        gb_price = p.price / p.traffic_gb if p.traffic_gb > 0 else 0
        type_badge = "🟣 Amnezia" if p.service_type == 'amnezia' else "⚙️ XUI"
        text += (
            f"\n{i}️⃣ <b>{p.name}</b> ({type_badge})\n"
            f"   📶 {p.traffic_gb} گیگابایت | ⏳ {p.duration_days} روز\n"
            f"   💰 {p.price:,} تومان"
        )
        if gb_price > 0:
            text += f" | ~{gb_price:,.0f} تومان/گیگ"
        text += "\n"
    text += "\n━━━━━━━━━━━━━━━━━━\n⚠️ <i>برای ویرایش یا حذف هر پلن، روی دکمه مربوطه کلیک کنید:</i>"
    kb_buttons = []
    for p in plans:
        kb_buttons.append([
            InlineKeyboardButton(text=f"✏️ ویرایش «{p.name}»", callback_data=f"editplan_{p.id}"),
            InlineKeyboardButton(text=f"🗑 حذف «{p.name}»", callback_data=f"delplan_{p.id}")
        ])
    kb_buttons.append([InlineKeyboardButton(text="⬅️ بازگشت به پنل", callback_data="admin_panel")])
    kb = InlineKeyboardMarkup(inline_keyboard=kb_buttons)
    await callback.message.edit_text(text, parse_mode="HTML", reply_markup=kb)

@dp.callback_query(F.data.startswith("delplan_"))
async def delete_plan_action(callback: types.CallbackQuery):
    if callback.from_user.id not in get_admin_ids(): return
    plan_id = int(callback.data.split("_")[1])
    with SessionLocal() as db:
        plan = db.query(Plan).filter(Plan.id == plan_id).first()
        if plan:
            plan.is_active = False
            db.commit()
            await callback.answer(f"✅ پلن '{plan.name}' با موفقیت حذف (غیرفعال) شد.", show_alert=True)
    await view_active_plans(callback)

# ==============================================================================
# ADMIN: EDIT PLAN FLOW
# ==============================================================================
@dp.callback_query(F.data.startswith("editplan_"))
async def edit_plan_start(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in get_admin_ids():
        return await callback.answer("⛔ دسترسی غیرمجاز", show_alert=True)
    plan_id = int(callback.data.split("_")[1])
    with SessionLocal() as db:
        plan = db.query(Plan).filter(Plan.id == plan_id).first()
        if not plan:
            await callback.answer("❌ پلن یافت نشد.", show_alert=True)
            return
        await state.update_data(plan_id=plan_id, plan=plan)
    await callback.message.edit_text(
        f"✏️ <b>ویرایش پلن</b>\n━━━━━━━━━━━━━━━━━━\n"
        f"نام فعلی: <b>{plan.name}</b>\n\n"
        f"نام جدید را وارد کنید (یا برای رد شدن از این مرحله دکمه زیر را بزنید):",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⏭ رد شدن (بدون تغییر)", callback_data="edit_skip_name")]
        ]),
        parse_mode="HTML"
    )
    await state.set_state(EditPlanFlow.wait_for_name)

@dp.callback_query(F.data == "edit_skip_name", EditPlanFlow.wait_for_name)
async def edit_skip_name(callback: types.CallbackQuery, state: FSMContext):
    await state.update_data(new_name=None)
    await callback.answer()
    await edit_plan_ask_gb(callback.message, state)

@dp.message(EditPlanFlow.wait_for_name)
async def edit_plan_name(message: types.Message, state: FSMContext):
    if message.from_user.id not in get_admin_ids():
        return await state.clear()
    name = message.text.strip()
    if not name:
        await message.answer("⚠️ نام نمی‌تواند خالی باشد. دوباره وارد کنید یا رد شوید.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⏭ رد شدن", callback_data="edit_skip_name")]]))
        return
    await state.update_data(new_name=name)
    await delete_user_message(bot, message)
    await edit_plan_ask_gb(message, state)

async def edit_plan_ask_gb(msg: types.Message, state: FSMContext):
    data = await state.get_data()
    plan = data.get('plan')
    current = plan.traffic_gb
    await msg.answer(
        f"📶 <b>حجم پلن</b>\n━━━━━━━━━━━━━━━━━━\n"
        f"حجم فعلی: <b>{current} گیگابایت</b>\n\n"
        f"حجم جدید را به گیگابایت وارد کنید (یا رد شوید):",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⏭ رد شدن (بدون تغییر)", callback_data="edit_skip_gb")]
        ]),
        parse_mode="HTML"
    )
    await state.set_state(EditPlanFlow.wait_for_gb)

@dp.callback_query(F.data == "edit_skip_gb", EditPlanFlow.wait_for_gb)
async def edit_skip_gb(callback: types.CallbackQuery, state: FSMContext):
    await state.update_data(new_gb=None)
    await callback.answer()
    await edit_plan_ask_days(callback.message, state)

@dp.message(EditPlanFlow.wait_for_gb)
async def edit_plan_gb(message: types.Message, state: FSMContext):
    if message.from_user.id not in get_admin_ids():
        return await state.clear()
    if not message.text.isdigit() or int(message.text) <= 0:
        await message.answer("⚠️ لطفاً یک عدد مثبت وارد کنید یا رد شوید.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⏭ رد شدن", callback_data="edit_skip_gb")]]))
        return
    gb = int(message.text)
    await state.update_data(new_gb=gb)
    await delete_user_message(bot, message)
    await edit_plan_ask_days(message, state)

async def edit_plan_ask_days(msg: types.Message, state: FSMContext):
    data = await state.get_data()
    plan = data.get('plan')
    current = plan.duration_days
    await msg.answer(
        f"⏳ <b>مدت پلن</b>\n━━━━━━━━━━━━━━━━━━\n"
        f"مدت فعلی: <b>{current} روز</b>\n\n"
        f"مدت جدید را به روز وارد کنید (یا رد شوید):",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⏭ رد شدن (بدون تغییر)", callback_data="edit_skip_days")]
        ]),
        parse_mode="HTML"
    )
    await state.set_state(EditPlanFlow.wait_for_days)

@dp.callback_query(F.data == "edit_skip_days", EditPlanFlow.wait_for_days)
async def edit_skip_days(callback: types.CallbackQuery, state: FSMContext):
    await state.update_data(new_days=None)
    await callback.answer()
    await edit_plan_ask_price(callback.message, state)

@dp.message(EditPlanFlow.wait_for_days)
async def edit_plan_days(message: types.Message, state: FSMContext):
    if message.from_user.id not in get_admin_ids():
        return await state.clear()
    if not message.text.isdigit() or int(message.text) <= 0:
        await message.answer("⚠️ لطفاً یک عدد مثبت وارد کنید یا رد شوید.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⏭ رد شدن", callback_data="edit_skip_days")]]))
        return
    days = int(message.text)
    await state.update_data(new_days=days)
    await delete_user_message(bot, message)
    await edit_plan_ask_price(message, state)

async def edit_plan_ask_price(msg: types.Message, state: FSMContext):
    data = await state.get_data()
    plan = data.get('plan')
    current = plan.price
    await msg.answer(
        f"💰 <b>قیمت پلن</b>\n━━━━━━━━━━━━━━━━━━\n"
        f"قیمت فعلی: <b>{current:,} تومان</b>\n\n"
        f"قیمت جدید را به تومان وارد کنید (یا رد شوید):",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⏭ رد شدن (بدون تغییر)", callback_data="edit_skip_price")]
        ]),
        parse_mode="HTML"
    )
    await state.set_state(EditPlanFlow.wait_for_price)

@dp.callback_query(F.data == "edit_skip_price", EditPlanFlow.wait_for_price)
async def edit_skip_price(callback: types.CallbackQuery, state: FSMContext):
    await state.update_data(new_price=None)
    await callback.answer()
    await edit_plan_finish(callback.message, state)

@dp.message(EditPlanFlow.wait_for_price)
async def edit_plan_price(message: types.Message, state: FSMContext):
    if message.from_user.id not in get_admin_ids():
        return await state.clear()
    if not message.text.isdigit() or int(message.text) <= 0:
        await message.answer("⚠️ لطفاً یک عدد مثبت وارد کنید یا رد شوید.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⏭ رد شدن", callback_data="edit_skip_price")]]))
        return
    price = int(message.text)
    await state.update_data(new_price=price)
    await delete_user_message(bot, message)
    await edit_plan_finish(message, state)

async def edit_plan_finish(msg: types.Message, state: FSMContext):
    data = await state.get_data()
    plan = data.get('plan')
    new_name = data.get('new_name')
    new_gb = data.get('new_gb')
    new_days = data.get('new_days')
    new_price = data.get('new_price')
    # Update plan
    with SessionLocal() as db:
        p = db.query(Plan).filter(Plan.id == plan.id).first()
        if not p:
            await msg.answer("❌ پلن یافت نشد.", reply_markup=get_admin_menu())
            await state.clear()
            return
        if new_name is not None:
            p.name = new_name
        if new_gb is not None:
            p.traffic_gb = new_gb
        if new_days is not None:
            p.duration_days = new_days
        if new_price is not None:
            p.price = new_price
        db.commit()
    await state.clear()
    await msg.answer(
        "✅ <b>پلن با موفقیت ویرایش شد.</b>",
        reply_markup=get_admin_menu(),
        parse_mode="HTML"
    )

# ==============================================================================
# ADMIN: COUPON MANAGEMENT (FULL)
# ==============================================================================
@dp.callback_query(F.data == "admin_coupon_menu")
async def admin_coupon_menu(callback: types.CallbackQuery):
    if callback.from_user.id not in get_admin_ids(): return
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ ایجاد کد تخفیف جدید", callback_data="admin_add_coupon")],
        [InlineKeyboardButton(text="📋 لیست کدها", callback_data="admin_list_coupons")],
        [InlineKeyboardButton(text="⬅️ بازگشت", callback_data="admin_panel")]
    ])
    await callback.message.edit_text("🎫 <b>مدیریت کدهای تخفیف</b>", reply_markup=kb, parse_mode="HTML")

@dp.callback_query(F.data == "admin_add_coupon")
async def add_coupon_start(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in get_admin_ids(): return
    await callback.message.edit_text("📝 <b>کد تخفیف را وارد کنید:</b>\n(فقط حروف و اعداد انگلیسی، بدون فاصله)", reply_markup=get_cancel_kb(), parse_mode="HTML")
    await state.set_state(AdminFlow.wait_for_coupon_code)

@dp.message(AdminFlow.wait_for_coupon_code)
async def add_coupon_code(message: types.Message, state: FSMContext):
    code = message.text.strip().upper()
    if not re.match(r'^[A-Z0-9]+$', code):
        return await message.answer("⚠️ کد فقط می‌تواند شامل حروف بزرگ و اعداد باشد.", reply_markup=get_cancel_kb())
    with SessionLocal() as db:
        if db.query(Coupon).filter(Coupon.code == code).first():
            return await message.answer("❌ این کد قبلاً وجود دارد.", reply_markup=get_cancel_kb())
        await state.update_data(coupon_code=code)
    await message.answer("📊 <b>نوع تخفیف را انتخاب کنید:</b>", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="درصدی", callback_data="coupon_type_percent")],
        [InlineKeyboardButton(text="مبلغ ثابت (تومان)", callback_data="coupon_type_fixed")]
    ]), parse_mode="HTML")
    await state.set_state(AdminFlow.wait_for_coupon_type)

@dp.callback_query(F.data.startswith("coupon_type_"))
async def add_coupon_type(callback: types.CallbackQuery, state: FSMContext):
    discount_type = callback.data.split("_")[2]
    await state.update_data(discount_type=discount_type)
    unit = "%" if discount_type == "percent" else "تومان"
    await callback.message.edit_text(f"🔢 <b>مقدار تخفیف را وارد کنید (به {unit}):</b>", reply_markup=get_cancel_kb(), parse_mode="HTML")
    await state.set_state(AdminFlow.wait_for_coupon_discount)

@dp.message(AdminFlow.wait_for_coupon_discount)
async def add_coupon_discount(message: types.Message, state: FSMContext):
    if not message.text.isdigit() or int(message.text) <= 0:
        return await message.answer("⚠️ لطفاً یک عدد مثبت وارد کنید.", reply_markup=get_cancel_kb())
    await state.update_data(discount_value=int(message.text))
    await message.answer("🔢 <b>حداکثر تعداد استفاده (کل) را وارد کنید (0 = نامحدود):</b>", reply_markup=get_cancel_kb(), parse_mode="HTML")
    await state.set_state(AdminFlow.wait_for_coupon_max_total)

@dp.message(AdminFlow.wait_for_coupon_max_total)
async def add_coupon_max_total(message: types.Message, state: FSMContext):
    if not message.text.isdigit():
        return await message.answer("⚠️ لطفاً یک عدد وارد کنید.", reply_markup=get_cancel_kb())
    await state.update_data(max_uses_total=int(message.text))
    await message.answer("👤 <b>حداکثر استفاده برای هر کاربر (0 = نامحدود):</b>", reply_markup=get_cancel_kb(), parse_mode="HTML")
    await state.set_state(AdminFlow.wait_for_coupon_max_per_user)

@dp.message(AdminFlow.wait_for_coupon_max_per_user)
async def add_coupon_max_per_user(message: types.Message, state: FSMContext):
    if not message.text.isdigit():
        return await message.answer("⚠️ لطفاً یک عدد وارد کنید.", reply_markup=get_cancel_kb())
    await state.update_data(max_uses_per_user=int(message.text))
    await message.answer("📅 <b>تاریخ انقضا (اختیاری، فرمت: YYYY-MM-DD)</b>\nاگر نمی‌خواهید محدودیت زمانی داشته باشید، عدد 0 را وارد کنید:", reply_markup=get_cancel_kb(), parse_mode="HTML")
    await state.set_state(AdminFlow.wait_for_coupon_expiry)

@dp.message(AdminFlow.wait_for_coupon_expiry)
async def add_coupon_expiry(message: types.Message, state: FSMContext):
    expiry = None
    if message.text.strip() != '0':
        try:
            expiry = datetime.strptime(message.text.strip(), '%Y-%m-%d')
        except ValueError:
            return await message.answer("⚠️ فرمت تاریخ نامعتبر. لطفاً به فرمت YYYY-MM-DD وارد کنید یا 0 برای بدون محدودیت.", reply_markup=get_cancel_kb())
    await state.update_data(expiry_date=expiry, applicable_selected=set())
    # Send a new bot message — can't edit the user's expiry message
    sent = await message.answer("🔄 در حال آماده‌سازی...", parse_mode="HTML")
    await show_coupon_applicable_menu(sent, state)

async def show_coupon_applicable_menu(msg: types.Message, state: FSMContext):
    """Show the applicable purchase types selection. `msg` must be a bot message."""
    data = await state.get_data()
    selected = data.get('applicable_selected', set())
    kb_buttons = []
    all_selected = (len(selected) == 3)
    for key, label in [("new", "خرید جدید"), ("renewal", "تمدید"), ("topup", "حجم اضافه")]:
        mark = "✅" if key in selected else "🔘"
        kb_buttons.append([InlineKeyboardButton(text=f"{mark} {label}", callback_data=f"cpn_toggle_{key}")])
    kb = InlineKeyboardMarkup(inline_keyboard=[
        *kb_buttons,
        [InlineKeyboardButton(text="✅ همه موارد" if all_selected else "🔘 همه موارد", callback_data="cpn_toggle_all")],
        [InlineKeyboardButton(text="✔️ تایید نهایی", callback_data="cpn_confirm")],
        [InlineKeyboardButton(text="❌ انصراف", callback_data="cancel")]
    ])
    try:
        await msg.edit_text("🎯 <b>نوع خریدهای قابل استفاده را انتخاب کنید:</b>\n(می‌توانید چند گزینه را انتخاب کنید)", reply_markup=kb, parse_mode="HTML")
    except Exception:
        # Fallback if edit fails (e.g. message type mismatch) — send new message
        await msg.delete()
        msg = await msg.answer("🎯 <b>نوع خریدهای قابل استفاده را انتخاب کنید:</b>\n(می‌توانید چند گزینه را انتخاب کنید)", reply_markup=kb, parse_mode="HTML")
    await state.set_state(AdminFlow.wait_for_coupon_applicable)

@dp.callback_query(AdminFlow.wait_for_coupon_applicable, F.data.startswith("cpn_toggle_"))
async def toggle_coupon_applicable(callback: types.CallbackQuery, state: FSMContext):
    val = callback.data.split("_")[2]
    data = await state.get_data()
    selected = data.get('applicable_selected', set())
    if val == "all":
        # Toggle all: if already all selected, deselect all; otherwise select all
        if len(selected) == 4:
            selected = set()
        else:
            selected = {"new", "renewal", "topup", "pack"}
    else:
        if val in selected:
            selected.remove(val)
        else:
            selected.add(val)
    await state.update_data(applicable_selected=selected)
    await show_coupon_applicable_menu(callback.message, state)

@dp.callback_query(AdminFlow.wait_for_coupon_applicable, F.data == "cpn_confirm")
async def confirm_coupon_applicable(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    selected = data.get('applicable_selected', set())
    if not selected:
        return await callback.answer("⚠️ حداقل یک گزینه را انتخاب کنید!", show_alert=True)
    applicable = ','.join(sorted(selected))
    with SessionLocal() as db:
        coupon = Coupon(
            code=data['coupon_code'],
            discount_type=data['discount_type'],
            discount_value=data['discount_value'],
            max_uses_total=data['max_uses_total'],
            max_uses_per_user=data['max_uses_per_user'],
            expiry_date=data['expiry_date'],
            applicable_to=applicable
        )
        db.add(coupon)
        db.commit()
    await state.clear()
    await callback.message.edit_text(f"✅ <b>کد تخفیف {data['coupon_code']} با موفقیت ایجاد شد!</b>", reply_markup=get_admin_menu(), parse_mode="HTML")

@dp.callback_query(F.data == "admin_list_coupons")
async def list_coupons(callback: types.CallbackQuery):
    if callback.from_user.id not in get_admin_ids(): return
    with SessionLocal() as db:
        coupons = db.query(Coupon).all()
    if not coupons:
        return await callback.message.edit_text(
            "🎫 <b>کدهای تخفیف</b>\n\n❌ هیچ کد تخفیفی وجود ندارد.\nبرای ایجاد کد جدید از دکمه «➕ ایجاد کد تخفیف جدید» استفاده کنید.",
            reply_markup=get_admin_menu(), parse_mode="HTML"
        )
    text = "🎫 <b>لیست کدهای تخفیف</b>\n━━━━━━━━━━━━━━━━━━\n"
    for c in coupons:
        discount_str = f"{c.discount_value}%" if c.discount_type == "percent" else f"{c.discount_value:,} تومان"
        expiry = c.expiry_date.strftime('%Y-%m-%d') if c.expiry_date else '♾️'
        status = "🟢" if c.active else "🔴"
        applicable_map = {'all': 'همه', 'new': 'خرید جدید', 'renewal': 'تمدید', 'topup': 'حجم اضافه', 'pack': 'خرید بسته ترافیک'}
        applicable_str = applicable_map.get(c.applicable_to, c.applicable_to) if c.applicable_to != 'all' and ',' not in c.applicable_to else 'همه'
        # Count total uses
        usage_count = db.query(CouponUsage).filter(CouponUsage.coupon_id == c.id).count()
        text += (
            f"\n{status} <b>{c.code}</b>\n"
            f"   ┣ 💰 تخفیف: {discount_str}\n"
            f"   ┣ 📊 کاربرد: {applicable_str}\n"
            f"   ┣ 👥 کل/کاربر: {c.max_uses_total or '∞'}/{c.max_uses_per_user or '∞'}\n"
            f"   ┣ 📊 استفاده: {usage_count}\n"
            f"   ┗ 📅 انقضا: {expiry}\n"
        )
    text += "\n━━━━━━━━━━━━━━━━━━\n👇 برای حذف هر کد، روی دکمه مربوطه کلیک کنید:"
    kb_buttons = []
    for c in coupons:
        kb_buttons.append([InlineKeyboardButton(text=f"🗑 حذف {c.code}", callback_data=f"admin_delete_coupon_{c.id}")])
    kb_buttons.append([InlineKeyboardButton(text="🔄 تغییر وضعیت کد", callback_data="admin_toggle_coupon")])
    kb_buttons.append([InlineKeyboardButton(text="⬅️ بازگشت به منوی تخفیف", callback_data="admin_coupon_menu")])
    kb = InlineKeyboardMarkup(inline_keyboard=kb_buttons)
    await callback.message.edit_text(text, parse_mode="HTML", reply_markup=kb)

@dp.callback_query(F.data == "admin_toggle_coupon")
async def toggle_coupon_start(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in get_admin_ids(): return
    await callback.message.edit_text("📝 <b>کد تخفیف مورد نظر را وارد کنید تا وضعیت آن تغییر کند:</b>", reply_markup=get_cancel_kb(), parse_mode="HTML")
    await state.set_state(AdminFlow.wait_for_coupon_toggle)

@dp.message(AdminFlow.wait_for_coupon_toggle)
async def toggle_coupon_execute(message: types.Message, state: FSMContext):
    code = message.text.strip().upper()
    with SessionLocal() as db:
        coupon = db.query(Coupon).filter(Coupon.code == code).first()
        if not coupon:
            await message.answer("❌ کد تخفیف یافت نشد.", reply_markup=get_cancel_kb())
            return
        coupon.active = not coupon.active
        db.commit()
    await state.clear()
    await message.answer(f"✅ وضعیت کد {code} به {'فعال' if coupon.active else 'غیرفعال'} تغییر یافت.", reply_markup=get_admin_menu(), parse_mode="HTML")

@dp.callback_query(F.data.startswith("admin_delete_coupon_"))
async def admin_delete_coupon_confirm(callback: types.CallbackQuery):
    if callback.from_user.id not in get_admin_ids():
        return await callback.answer("⛔ دسترسی غیرمجاز", show_alert=True)
    coupon_id = int(callback.data.split("_")[3])
    with SessionLocal() as db:
        coupon = db.query(Coupon).filter(Coupon.id == coupon_id).first()
        if not coupon:
            return await callback.answer("❌ کد تخفیف یافت نشد.", show_alert=True)
        text = (
            f"⚠️ <b>آیا از حذف کد تخفیف <code>{coupon.code}</code> اطمینان دارید؟</b>\n\n"
            f"این عملیات غیرقابل بازگشت است و تمام سوابق استفاده از این کد نیز حذف خواهند شد."
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🗑 بله، حذف کن", callback_data=f"confirm_admin_delete_coupon_{coupon_id}")],
            [InlineKeyboardButton(text="❌ انصراف", callback_data="admin_list_coupons")]
        ])
        await callback.message.edit_text(text, reply_markup=kb, parse_mode="HTML")

@dp.callback_query(F.data.startswith("confirm_admin_delete_coupon_"))
async def confirm_admin_delete_coupon(callback: types.CallbackQuery):
    if callback.from_user.id not in get_admin_ids():
        return await callback.answer("⛔ دسترسی غیرمجاز", show_alert=True)
    coupon_id = int(callback.data.split("_")[4])
    with SessionLocal() as db:
        # Delete all usages first
        db.query(CouponUsage).filter(CouponUsage.coupon_id == coupon_id).delete()
        # Delete the coupon
        coupon = db.query(Coupon).filter(Coupon.id == coupon_id).first()
        if coupon:
            db.delete(coupon)
            db.commit()
            await callback.answer(f"✅ کد {coupon.code} با موفقیت حذف شد.", show_alert=True)
        else:
            await callback.answer("❌ کد تخفیف یافت نشد.", show_alert=True)
    # Refresh the list
    await list_coupons(callback)

# ==============================================================================
# ADMIN: TRIAL SETTINGS
# ==============================================================================
@dp.callback_query(F.data == "admin_trial_settings")
async def admin_trial_settings(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in get_admin_ids(): return
    with SessionLocal() as db:
        traffic = db.query(AppSetting).filter(AppSetting.key == 'trial_traffic_gb').first()
        days = db.query(AppSetting).filter(AppSetting.key == 'trial_duration_days').first()
        current_traffic = float(traffic.value) if traffic else 0.1
        current_days = int(days.value) if days else 1
    traffic_display = f"{current_traffic:.1f}".rstrip('0').rstrip('.') if current_traffic < 1 else str(int(current_traffic))
    await callback.message.edit_text(
        f"🎁 <b>تنظیمات تست رایگان</b>\n━━━━━━━━━━━━━━━━━━\nحجم فعلی: {traffic_display} GB\nمدت فعلی: {current_days} روز\n\nبرای تغییر، دکمه زیر را بزنید:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✏️ تغییر حجم", callback_data="admin_set_trial_traffic")],
            [InlineKeyboardButton(text="✏️ تغییر مدت", callback_data="admin_set_trial_days")],
            [InlineKeyboardButton(text="⬅️ بازگشت", callback_data="admin_panel")]
        ]), parse_mode="HTML"
    )

@dp.callback_query(F.data == "admin_set_trial_traffic")
async def set_trial_traffic(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in get_admin_ids(): return
    await callback.message.edit_text("📶 <b>حجم تست رایگان را به گیگابایت وارد کنید:</b>", reply_markup=get_cancel_kb(), parse_mode="HTML")
    await state.set_state(AdminFlow.wait_for_trial_traffic)

@dp.message(AdminFlow.wait_for_trial_traffic)
async def save_trial_traffic(message: types.Message, state: FSMContext):
    try:
        val = float(message.text)
        if val <= 0: raise ValueError
    except ValueError:
        return await message.answer("⚠️ لطفاً یک عدد مثبت وارد کنید (مثال: 0.1 برای 100 مگابایت).", reply_markup=get_cancel_kb())
    with SessionLocal() as db:
        setting = db.query(AppSetting).filter(AppSetting.key == 'trial_traffic_gb').first()
        if setting: setting.value = str(val)
        else: db.add(AppSetting(key='trial_traffic_gb', value=str(val)))
        db.commit()
    await state.clear()
    traffic_display = f"{val:.1f}".rstrip('0').rstrip('.') if val < 1 else str(int(val))
    await message.answer(f"✅ <b>حجم تست رایگان به {traffic_display} GB تغییر یافت.</b>", reply_markup=get_admin_menu(), parse_mode="HTML")

@dp.callback_query(F.data == "admin_set_trial_days")
async def set_trial_days(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in get_admin_ids(): return
    await callback.message.edit_text("⏳ <b>مدت تست رایگان را به روز وارد کنید:</b>", reply_markup=get_cancel_kb(), parse_mode="HTML")
    await state.set_state(AdminFlow.wait_for_trial_days)

@dp.message(AdminFlow.wait_for_trial_days)
async def save_trial_days(message: types.Message, state: FSMContext):
    if not message.text.isdigit() or int(message.text) <= 0:
        return await message.answer("⚠️ لطفاً یک عدد مثبت وارد کنید.", reply_markup=get_cancel_kb())
    with SessionLocal() as db:
        setting = db.query(AppSetting).filter(AppSetting.key == 'trial_duration_days').first()
        if setting: setting.value = message.text
        else: db.add(AppSetting(key='trial_duration_days', value=message.text))
        db.commit()
    await state.clear()
    await message.answer(f"✅ <b>مدت تست رایگان به {message.text} روز تغییر یافت.</b>", reply_markup=get_admin_menu(), parse_mode="HTML")

# ==============================================================================
# ADMIN: REFERRAL SETTINGS
# ==============================================================================
@dp.callback_query(F.data == "admin_referral_settings")
async def admin_referral_settings(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in get_admin_ids(): return
    with SessionLocal() as db:
        threshold = db.query(AppSetting).filter(AppSetting.key == 'referral_threshold').first()
        plan_id = db.query(AppSetting).filter(AppSetting.key == 'referral_reward_plan_id').first()
        plan = None
        if plan_id:
            plan = db.query(Plan).filter(Plan.id == int(plan_id.value)).first()
    text = f"🤝 <b>تنظیمات برنامه معرفی</b>\n━━━━━━━━━━━━━━━━━━\nآستانه دعوت: {threshold.value if threshold else '10'}\nپلن جایزه: {plan.name if plan else 'تنظیم نشده'}"
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✏️ تغییر آستانه", callback_data="admin_set_referral_threshold")],
        [InlineKeyboardButton(text="✏️ تغییر پلن جایزه", callback_data="admin_set_referral_plan")],
        [InlineKeyboardButton(text="⬅️ بازگشت", callback_data="admin_panel")]
    ])
    await callback.message.edit_text(text, reply_markup=kb, parse_mode="HTML")

@dp.callback_query(F.data == "admin_set_referral_threshold")
async def set_referral_threshold(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in get_admin_ids(): return
    await callback.message.edit_text("🔢 <b>تعداد دعوت‌های موفق مورد نیاز برای دریافت جایزه را وارد کنید:</b>", reply_markup=get_cancel_kb(), parse_mode="HTML")
    await state.set_state(AdminFlow.wait_for_referral_threshold)

@dp.message(AdminFlow.wait_for_referral_threshold)
async def save_referral_threshold(message: types.Message, state: FSMContext):
    if not message.text.isdigit() or int(message.text) <= 0:
        return await message.answer("⚠️ لطفاً یک عدد مثبت وارد کنید.", reply_markup=get_cancel_kb())
    with SessionLocal() as db:
        setting = db.query(AppSetting).filter(AppSetting.key == 'referral_threshold').first()
        if setting: setting.value = message.text
        else: db.add(AppSetting(key='referral_threshold', value=message.text))
        db.commit()
    await state.clear()
    await message.answer(f"✅ <b>آستانه دعوت به {message.text} تغییر یافت.</b>", reply_markup=get_admin_menu(), parse_mode="HTML")

@dp.callback_query(F.data == "admin_set_referral_plan")
async def set_referral_plan(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in get_admin_ids(): return
    with SessionLocal() as db:
        plans = db.query(Plan).filter(Plan.is_active == True).all()
    if not plans:
        return await callback.answer("هیچ پلن فعالی وجود ندارد. ابتدا یک پلن ایجاد کنید.", show_alert=True)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"{p.name} (ID: {p.id})", callback_data=f"set_referral_plan_{p.id}")] for p in plans
    ] + [[InlineKeyboardButton(text="❌ انصراف", callback_data="cancel")]])
    await callback.message.edit_text("🎁 <b>پلن جایزه را انتخاب کنید:</b>", reply_markup=kb, parse_mode="HTML")
    await state.set_state(AdminFlow.wait_for_referral_reward_plan)

@dp.callback_query(AdminFlow.wait_for_referral_reward_plan, F.data.startswith("set_referral_plan_"))
async def save_referral_plan(callback: types.CallbackQuery, state: FSMContext):
    plan_id = int(callback.data.split("_")[3])
    with SessionLocal() as db:
        setting = db.query(AppSetting).filter(AppSetting.key == 'referral_reward_plan_id').first()
        if setting: setting.value = str(plan_id)
        else: db.add(AppSetting(key='referral_reward_plan_id', value=str(plan_id)))
        db.commit()
    await state.clear()
    await callback.message.edit_text(f"✅ <b>پلن جایزه با ID {plan_id} انتخاب شد.</b>", reply_markup=get_admin_menu(), parse_mode="HTML")

# ==============================================================================
# ADMIN: BROADCAST (message to all bot users)
# ==============================================================================
@dp.callback_query(F.data == "admin_broadcast")
async def admin_broadcast_start(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in get_admin_ids(): return
    await state.set_state(AdminFlow.wait_for_broadcast_message)
    await callback.message.edit_text(
        "📢 <b>پیام همگانی</b>\n"
        "━━━━━━━━━━━━━━━━━━\n"
        "متن پیامی که می‌خواهید برای <b>همه کاربران</b> ارسال شود را بنویسید.\n\n"
        "▫️ قالب‌بندی HTML پشتیبانی می‌شود (<code>&lt;b&gt;</code>, <code>&lt;i&gt;</code>, ...)\n"
        "▫️ پس از ارسال، پیش‌نمایش و تعداد گیرندگان نمایش داده می‌شود.",
        reply_markup=get_cancel_kb(), parse_mode="HTML"
    )

@dp.message(AdminFlow.wait_for_broadcast_message)
async def admin_broadcast_preview(message: types.Message, state: FSMContext):
    if message.from_user.id not in get_admin_ids(): return
    text = message.html_text or message.text
    if not text:
        return await message.answer("⚠️ لطفاً یک پیام متنی ارسال کنید.", reply_markup=get_cancel_kb())
    recipients = get_all_user_ids()
    await state.update_data(broadcast_text=text)
    await state.set_state(None)  # leave the input state; await confirm callback
    preview = (
        f"📢 <b>پیش‌نمایش پیام همگانی</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"👥 تعداد گیرندگان: <b>{len(recipients)}</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n\n"
        f"{text}\n\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"⚠️ این عملیات قابل بازگشت نیست. آیا ارسال شود؟"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ ارسال به همه", callback_data="broadcast_confirm")],
        [InlineKeyboardButton(text="❌ انصراف", callback_data="cancel")]
    ])
    await message.answer(preview, reply_markup=kb, parse_mode="HTML")

@dp.callback_query(F.data == "broadcast_confirm")
async def admin_broadcast_confirm(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in get_admin_ids(): return
    data = await state.get_data()
    text = data.get('broadcast_text')
    await state.clear()
    if not text:
        return await callback.answer("⚠️ پیام منقضی شده است. دوباره تلاش کنید.", show_alert=True)
    tasks.broadcast_message.delay(callback.from_user.id, text)
    try: await callback.answer()
    except Exception as e: logger.debug(f"Callback answer failed: {e}")
    await callback.message.edit_text(
        "🚀 <b>ارسال پیام همگانی آغاز شد.</b>\n\n"
        "ارسال در پس‌زمینه با محدودیت نرخ انجام می‌شود. پس از پایان، گزارش تعداد ارسال‌های موفق/ناموفق برای شما ارسال خواهد شد.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ بازگشت به پنل", callback_data="admin_panel")]]),
        parse_mode="HTML"
    )

# ==============================================================================
# ADMIN: RESELLER MANAGEMENT
# ==============================================================================
@dp.callback_query(F.data == "admin_reseller_menu")
async def admin_reseller_menu(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in get_admin_ids(): return
    await state.clear()
    with SessionLocal() as db:
        days_setting = db.query(AppSetting).filter(AppSetting.key == 'reseller_service_days').first()
        ib_setting = db.query(AppSetting).filter(AppSetting.key == 'reseller_inbound_ids').first()
        count = db.query(Reseller).count()
    days = days_setting.value if days_setting else '30'
    ib_val = (ib_setting.value if ib_setting else '') or 'همه سرورها'
    text = (
        f"🧑‍💼 <b>مدیریت نمایندگان</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"تعداد نمایندگان: <b>{count}</b>\n"
        f"⏳ مدت سرویس نماینده: <b>{days} روز</b>\n"
        f"🖧 سرورهای مجاز: <b>{ib_val}</b>"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ افزودن نماینده", callback_data="admin_add_reseller")],
        [InlineKeyboardButton(text="📋 لیست نمایندگان", callback_data="admin_list_resellers")],
        [InlineKeyboardButton(text="⏳ مدت سرویس نماینده", callback_data="admin_set_reseller_days")],
        [InlineKeyboardButton(text="🖧 سرورهای نماینده", callback_data="admin_set_reseller_inbounds")],
        [InlineKeyboardButton(text="⬅️ بازگشت به پنل", callback_data="admin_panel")]
    ])
    await callback.message.edit_text(text, reply_markup=kb, parse_mode="HTML")

@dp.callback_query(F.data == "admin_add_reseller")
async def admin_add_reseller_start(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in get_admin_ids(): return
    await callback.message.edit_text(
        "👤 <b>شناسه عددی تلگرام نماینده را وارد کنید:</b>\n(فقط عدد — مثال: <code>123456789</code>)",
        reply_markup=get_cancel_kb(), parse_mode="HTML"
    )
    await state.set_state(AdminFlow.wait_for_reseller_id)

@dp.message(AdminFlow.wait_for_reseller_id)
async def admin_add_reseller_id(message: types.Message, state: FSMContext):
    if message.from_user.id not in get_admin_ids(): return
    text = message.text.strip()
    if not text.isdigit():
        return await message.answer("⚠️ لطفاً فقط شناسه عددی وارد کنید.", reply_markup=get_cancel_kb())
    rid = int(text)
    with SessionLocal() as db:
        rec = db.query(Reseller).filter(Reseller.telegram_user_id == rid).first()
        if rec:
            rec.is_active = True
        else:
            db.add(Reseller(telegram_user_id=rid, allowance_bytes=0, used_bytes=0, is_active=True))
        db.commit()
    await state.update_data(reseller_id=rid)
    await message.answer(
        f"✅ نماینده <code>{rid}</code> ثبت/فعال شد.\n\n"
        f"📦 <b>اعتبار ترافیک (گیگابایت) را برای افزودن وارد کنید:</b>\n(فقط عدد)",
        reply_markup=get_cancel_kb(), parse_mode="HTML"
    )
    await state.set_state(AdminFlow.wait_for_reseller_allowance)

@dp.message(AdminFlow.wait_for_reseller_allowance)
async def admin_add_reseller_allowance(message: types.Message, state: FSMContext):
    if message.from_user.id not in get_admin_ids(): return
    if not message.text.strip().isdigit():
        return await message.answer("⚠️ لطفاً فقط عدد وارد کنید.", reply_markup=get_cancel_kb())
    gb = int(message.text.strip())
    await state.update_data(pending_gb=gb)
    await message.answer(
        f"⏳ <b>مدت اعتبار این بسته ترافیک را به روز وارد کنید:</b>\n(عدد مثبت)",
        reply_markup=get_cancel_kb(), parse_mode="HTML"
    )
    await state.set_state(AdminFlow.wait_for_reseller_pack_days)

@dp.message(AdminFlow.wait_for_reseller_pack_days)
async def admin_add_reseller_pack_days(message: types.Message, state: FSMContext):
    if message.from_user.id not in get_admin_ids(): return
    if not message.text.strip().isdigit() or int(message.text.strip()) <= 0:
        return await message.answer("⚠️ لطفاً یک عدد مثبت برای مدت (روز) وارد کنید.", reply_markup=get_cancel_kb())
    days = int(message.text.strip())
    data = await state.get_data()
    rid = data['reseller_id']
    gb = data['pending_gb']
    expiry_date = datetime.now(timezone.utc) + timedelta(days=days)
    with SessionLocal() as db:
        pack = ResellerPack(
            reseller_id=rid,
            granted_bytes=gb * GB,
            used_bytes=0,
            expiry_date=expiry_date,
            is_active=True
        )
        db.add(pack)
        db.commit()
    await state.clear()
    await message.answer(
        f"✅ <b>{gb} گیگابایت اعتبار به نماینده <code>{rid}</code> اضافه شد.</b>\n"
        f"⏳ مدت اعتبار: {days} روز (تا {expiry_date.strftime('%Y-%m-%d')})",
        reply_markup=get_admin_menu(), parse_mode="HTML"
    )

@dp.callback_query(F.data == "admin_list_resellers")
async def admin_list_resellers(callback: types.CallbackQuery):
    if callback.from_user.id not in get_admin_ids(): return
    with SessionLocal() as db:
        resellers = db.query(Reseller).order_by(Reseller.created_at.desc()).all()
        # For each reseller, compute pack stats
        data = []
        for r in resellers:
            packs = db.query(ResellerPack).filter(ResellerPack.reseller_id == r.telegram_user_id).all()
            total_granted = sum(p.granted_bytes for p in packs)
            total_used = sum(p.used_bytes for p in packs)
            now = datetime.now(timezone.utc)
            remaining = sum(p.granted_bytes - p.used_bytes for p in packs if p.is_active and p.expiry_date > now)
            data.append((r.telegram_user_id, total_granted, total_used, remaining, r.is_active))
    if not data:
        return await callback.message.edit_text(
            "🧑‍💼 <b>نمایندگان</b>\n\n❌ هیچ نماینده‌ای ثبت نشده است.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="➕ افزودن نماینده", callback_data="admin_add_reseller")],
                [InlineKeyboardButton(text="⬅️ بازگشت", callback_data="admin_reseller_menu")]
            ]), parse_mode="HTML"
        )
    # Fetch chat info for each reseller to get username
    user_info = {}
    for rid, _, _, _, _ in data:
        try:
            chat = await bot.get_chat(rid)
            username = chat.username
            user_info[rid] = username
        except Exception:
            user_info[rid] = None
    text = "🧑‍💼 <b>لیست نمایندگان</b>\n━━━━━━━━━━━━━━━━━━\n"
    kb_buttons = []
    for rid, total_granted, total_used, remaining, active in data:
        status = "🟢" if active else "🔴"
        username = user_info.get(rid)
        if username:
            display = f"@{username} ({rid})"
        else:
            display = str(rid)
        text += f"\n{status} {display}\n   📦 باقیمانده: {format_size(remaining)} از {format_size(total_granted)} (کل مصرف: {format_size(total_used)})\n"
        kb_buttons.append([InlineKeyboardButton(text=f"{status} {display}", callback_data=f"reseller_detail_{rid}")])
    kb_buttons.append([InlineKeyboardButton(text="➕ افزودن نماینده", callback_data="admin_add_reseller")])
    kb_buttons.append([InlineKeyboardButton(text="⬅️ بازگشت", callback_data="admin_reseller_menu")])
    await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_buttons), parse_mode="HTML")


async def _show_reseller_detail(message, rid):
    with SessionLocal() as db:
        reseller = db.query(Reseller).filter(Reseller.telegram_user_id == rid).first()
        if not reseller:
            await message.edit_text("❌ نماینده یافت نشد.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ بازگشت", callback_data="admin_list_resellers")]]), parse_mode="HTML")
            return
        active = reseller.is_active
        # Fetch packs
        packs = db.query(ResellerPack).filter(ResellerPack.reseller_id == rid).order_by(ResellerPack.expiry_date.asc()).all()
        total_granted = sum(p.granted_bytes for p in packs)
        total_used = sum(p.used_bytes for p in packs)
        now = datetime.now(timezone.utc)
        remaining = sum(p.granted_bytes - p.used_bytes for p in packs if p.is_active and p.expiry_date > now)
    try:
        chat = await bot.get_chat(rid)
        username = chat.username
    except Exception:
        username = None
    display = f"@{username} ({rid})" if username else str(rid)
    status = "🟢 فعال" if active else "🔴 غیرفعال"
    text = (
        f"🧑‍💼 <b>مدیریت نماینده</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"شناسه: {display}\n"
        f"وضعیت: {status}\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"<b>اعتبار کل (بر اساس بسته‌ها):</b>\n"
        f"📦 باقیمانده: {format_size(remaining)} از {format_size(total_granted)} (کل مصرف: {format_size(total_used)})\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"<b>بسته‌های ترافیک:</b>\n"
    )
    if packs:
        for pack in packs:
            pack_remaining = max(0, pack.granted_bytes - pack.used_bytes)
            expiry_date = pack.expiry_date.strftime('%Y-%m-%d')
            now = datetime.now(timezone.utc)
            is_expired = pack.expiry_date < now or not pack.is_active
            status_icon = "🔴" if is_expired else ("🟢" if pack_remaining > 0 else "⚪")
            text += (
                f"{status_icon} {format_size(pack.granted_bytes)} | "
                f"مصرف: {format_size(pack.used_bytes)} | "
                f"باقی: {format_size(pack_remaining)} | "
                f"انقضا: {expiry_date}\n"
            )
    else:
        text += "❌ هیچ بسته‌ای وجود ندارد.\n"
    text += f"━━━━━━━━━━━━━━━━━━\nعملیات مورد نظر را انتخاب کنید:"
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ شارژ ترافیک", callback_data=f"res_grant_{rid}")],
        [InlineKeyboardButton(text="🔄 تغییر وضعیت", callback_data=f"res_toggle_{rid}")],
        [InlineKeyboardButton(text="🗑 حذف نماینده", callback_data=f"res_delete_{rid}")],
        [InlineKeyboardButton(text="⬅️ بازگشت به لیست", callback_data="admin_list_resellers")]
    ])
    await message.edit_text(text, reply_markup=kb, parse_mode="HTML")

@dp.callback_query(F.data.startswith("reseller_detail_"))
async def admin_reseller_detail(callback: types.CallbackQuery):
    if callback.from_user.id not in get_admin_ids(): return
    rid = int(callback.data.split("_")[2])
    await _show_reseller_detail(callback.message, rid)

@dp.callback_query(F.data.regexp(r'^res_delete_\d+$'))
async def admin_reseller_delete_confirm(callback: types.CallbackQuery):
    if callback.from_user.id not in get_admin_ids(): return
    rid = int(callback.data.split("_")[2])
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ بله، حذف شود", callback_data=f"res_delete_confirm_{rid}")],
        [InlineKeyboardButton(text="❌ انصراف", callback_data=f"res_detail_back_{rid}")]
    ])
    await callback.message.edit_text(
        f"⚠️ <b>آیا از حذف نماینده <code>{rid}</code> اطمینان دارید؟</b>\n\nاین عملیات غیرقابل بازگشت است.",
        reply_markup=kb, parse_mode="HTML"
    )

@dp.callback_query(F.data.regexp(r'^res_delete_confirm_\d+$'))
async def admin_reseller_delete_execute(callback: types.CallbackQuery):
    if callback.from_user.id not in get_admin_ids(): return
    rid = int(callback.data.split("_")[3])
    with SessionLocal() as db:
        reseller = db.query(Reseller).filter(Reseller.telegram_user_id == rid).first()
        if reseller:
            db.delete(reseller)
            db.commit()
            await callback.answer(f"✅ نماینده {rid} حذف شد.", show_alert=True)
        else:
            await callback.answer("❌ نماینده یافت نشد.", show_alert=True)
    await admin_list_resellers(callback)

@dp.callback_query(F.data.regexp(r'^res_detail_back_\d+$'))
async def admin_reseller_detail_back(callback: types.CallbackQuery):
    if callback.from_user.id not in get_admin_ids(): return
    rid = int(callback.data.split("_")[3])
    await _show_reseller_detail(callback.message, rid)

@dp.callback_query(F.data.startswith("res_grant_"))
async def admin_reseller_grant(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in get_admin_ids(): return
    rid = int(callback.data.split("_")[2])
    await state.update_data(reseller_id=rid)
    await callback.message.edit_text(
        f"📦 <b>اعتبار ترافیک (گیگابایت) برای افزودن به نماینده <code>{rid}</code> را وارد کنید:</b>\n(فقط عدد)",
        reply_markup=get_cancel_kb(), parse_mode="HTML"
    )
    await state.set_state(AdminFlow.wait_for_reseller_allowance)

@dp.callback_query(F.data.startswith("res_toggle_"))
async def admin_reseller_toggle(callback: types.CallbackQuery):
    if callback.from_user.id not in get_admin_ids(): return
    rid = int(callback.data.split("_")[2])
    with SessionLocal() as db:
        rec = db.query(Reseller).filter(Reseller.telegram_user_id == rid).first()
        if rec:
            rec.is_active = not rec.is_active
            db.commit()
            new_state = rec.is_active
            await callback.answer(f"وضعیت نماینده {rid}: {'فعال' if new_state else 'غیرفعال'}", show_alert=True)
    await admin_list_resellers(callback)

@dp.callback_query(F.data == "admin_set_reseller_days")
async def admin_set_reseller_days_start(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in get_admin_ids(): return
    await callback.message.edit_text(
        "⏳ <b>مدت سرویس نماینده را به روز وارد کنید:</b>\n(این مدت برای همه سرویس‌های ساخته‌شده توسط نمایندگان اعمال می‌شود)",
        reply_markup=get_cancel_kb(), parse_mode="HTML"
    )
    await state.set_state(AdminFlow.wait_for_reseller_duration)

@dp.message(AdminFlow.wait_for_reseller_duration)
async def admin_set_reseller_days_save(message: types.Message, state: FSMContext):
    if message.from_user.id not in get_admin_ids(): return
    if not message.text.strip().isdigit() or int(message.text.strip()) <= 0:
        return await message.answer("⚠️ لطفاً یک عدد مثبت وارد کنید.", reply_markup=get_cancel_kb())
    with SessionLocal() as db:
        setting = db.query(AppSetting).filter(AppSetting.key == 'reseller_service_days').first()
        if setting: setting.value = message.text.strip()
        else: db.add(AppSetting(key='reseller_service_days', value=message.text.strip()))
        db.commit()
    await state.clear()
    await message.answer(f"✅ <b>مدت سرویس نماینده به {message.text.strip()} روز تنظیم شد.</b>", reply_markup=get_admin_menu(), parse_mode="HTML")

@dp.callback_query(F.data == "admin_set_reseller_inbounds")
async def admin_set_reseller_inbounds_start(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in get_admin_ids(): return
    xui = XUIClient()
    try:
        inbounds = await xui.get_enabled_inbounds()
    except Exception as e:
        return await callback.answer(f"خطا در دریافت سرورها: {str(e)[:80]}", show_alert=True)
    with SessionLocal() as db:
        ib_setting = db.query(AppSetting).filter(AppSetting.key == 'reseller_inbound_ids').first()
        raw = (ib_setting.value if ib_setting else '') or ''
    selected = set(int(x) for x in raw.split(',') if x.strip().isdigit())
    await state.update_data(reseller_selected_inbounds=list(selected))
    await _render_reseller_inbounds(callback.message, inbounds, selected)
    await state.set_state(AdminFlow.wait_for_reseller_inbounds)

async def _render_reseller_inbounds(message, inbounds, selected):
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"{'🟢' if ib['id'] in selected else '🔘'} {ib['remark']} (Port {ib['port']})", callback_data=f"res_ib_{ib['id']}")]
        for ib in inbounds
    ] + [
        [InlineKeyboardButton(text="✔️ ذخیره", callback_data="res_ib_save")],
        [InlineKeyboardButton(text="❌ انصراف", callback_data="cancel")]
    ])
    await message.edit_text(
        "🖧 <b>سرورهای مجاز برای نمایندگان را انتخاب کنید:</b>\n(اگر هیچ‌کدام انتخاب نشود، همه سرورها استفاده می‌شوند)",
        reply_markup=kb, parse_mode="HTML"
    )

@dp.callback_query(AdminFlow.wait_for_reseller_inbounds, F.data.startswith("res_ib_"))
async def admin_toggle_reseller_inbound(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in get_admin_ids(): return
    part = callback.data.split("_", 2)[2]
    data = await state.get_data()
    selected = set(data.get('reseller_selected_inbounds', []))
    if part == "save":
        value = ','.join(str(i) for i in sorted(selected))
        with SessionLocal() as db:
            setting = db.query(AppSetting).filter(AppSetting.key == 'reseller_inbound_ids').first()
            if setting: setting.value = value
            else: db.add(AppSetting(key='reseller_inbound_ids', value=value))
            db.commit()
        await state.clear()
        shown = value if value else 'همه سرورها'
        return await callback.message.edit_text(
            f"✅ <b>سرورهای نماینده ذخیره شد:</b> {shown}",
            reply_markup=get_admin_menu(), parse_mode="HTML"
        )
    ib_id = int(part)
    if ib_id in selected: selected.remove(ib_id)
    else: selected.add(ib_id)
    await state.update_data(reseller_selected_inbounds=list(selected))
    xui = XUIClient()
    try:
        inbounds = await xui.get_inbounds()
    except Exception as e:
        return await callback.answer(f"خطا: {str(e)[:80]}", show_alert=True)
    await _render_reseller_inbounds(callback.message, inbounds, selected)


# ==============================================================================
# ADMIN: BILLING REPORT
# ==============================================================================
# ==============================================================================
# ADMIN: TRAFFIC PACK MANAGEMENT
# ==============================================================================

@dp.callback_query(F.data == "admin_traffic_packs")
async def admin_traffic_packs(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in get_admin_ids():
        return await callback.answer("⛔ دسترسی غیرمجاز", show_alert=True)
    await state.clear()
    with SessionLocal() as db:
        packs = db.query(TrafficPack).order_by(TrafficPack.id.desc()).all()
    if not packs:
        text = "📦 <b>مدیریت بسته‌های ترافیک</b>\n━━━━━━━━━━━━━━━━━━\n❌ هیچ بسته‌ای وجود ندارد.\nبرای افزودن بسته جدید از دکمه زیر استفاده کنید."
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="➕ افزودن بسته جدید", callback_data="admin_add_pack")],
            [InlineKeyboardButton(text="⬅️ بازگشت به پنل", callback_data="admin_panel")]
        ])
        await callback.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
        return
    text = "📦 <b>مدیریت بسته‌های ترافیک</b>\n━━━━━━━━━━━━━━━━━━\n"
    for p in packs:
        status = "🟢" if p.is_active else "🔴"
        text += f"{status} <b>{p.name}</b>\n   📶 {p.traffic_gb} GB | ⏳ {p.duration_days} روز | 💰 {p.price:,} تومان\n"
    text += "\n👇 برای مدیریت هر بسته، روی دکمه مربوطه کلیک کنید:"
    kb_buttons = []
    for p in packs:
        kb_buttons.append([
            InlineKeyboardButton(text=f"✏️ ویرایش {p.name}", callback_data=f"edit_pack_{p.id}"),
            InlineKeyboardButton(text=f"🗑 حذف {p.name}", callback_data=f"del_pack_{p.id}"),
            InlineKeyboardButton(text="🔄 فعال/غیرفعال" if p.is_active else "✅ فعال‌سازی", callback_data=f"toggle_pack_{p.id}")
        ])
    kb_buttons.append([InlineKeyboardButton(text="➕ افزودن بسته جدید", callback_data="admin_add_pack")])
    kb_buttons.append([InlineKeyboardButton(text="⬅️ بازگشت به پنل", callback_data="admin_panel")])
    kb = InlineKeyboardMarkup(inline_keyboard=kb_buttons)
    await callback.message.edit_text(text, reply_markup=kb, parse_mode="HTML")

@dp.callback_query(F.data == "admin_add_pack")
async def admin_add_pack_start(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in get_admin_ids():
        return await callback.answer("⛔ دسترسی غیرمجاز", show_alert=True)
    await state.update_data(editing_pack_id=None)
    await callback.message.edit_text(
        "🏷 <b>نام بسته ترافیک را وارد کنید:</b>\n<i>(مثال: 'بسته ۱۰ گیگ - ۱ ماهه')</i>",
        parse_mode="HTML",
        reply_markup=get_cancel_kb()
    )
    await state.set_state(AdminFlow.wait_for_pack_name)

@dp.message(AdminFlow.wait_for_pack_name)
async def admin_add_pack_name(message: types.Message, state: FSMContext):
    if message.from_user.id not in get_admin_ids():
        return await state.clear()
    await state.update_data(pack_name=message.text.strip())
    await delete_user_message(bot, message)
    await message.answer(
        "📶 <b>حجم بسته را به گیگابایت وارد کنید:</b>",
        parse_mode="HTML",
        reply_markup=get_cancel_kb()
    )
    await state.set_state(AdminFlow.wait_for_pack_gb)

@dp.message(AdminFlow.wait_for_pack_gb)
async def admin_add_pack_gb(message: types.Message, state: FSMContext):
    if message.from_user.id not in get_admin_ids():
        return await state.clear()
    if not message.text.isdigit() or int(message.text) <= 0:
        return await message.answer("⚠️ لطفاً یک عدد مثبت وارد کنید.", reply_markup=get_cancel_kb())
    await state.update_data(pack_gb=int(message.text))
    await delete_user_message(bot, message)
    await message.answer(
        "💰 <b>قیمت بسته را به تومان وارد کنید:</b>",
        parse_mode="HTML",
        reply_markup=get_cancel_kb()
    )
    await state.set_state(AdminFlow.wait_for_pack_price)

@dp.message(AdminFlow.wait_for_pack_price)
async def admin_add_pack_price(message: types.Message, state: FSMContext):
    if message.from_user.id not in get_admin_ids():
        return await state.clear()
    if not message.text.isdigit() or int(message.text) <= 0:
        return await message.answer("⚠️ لطفاً یک عدد مثبت وارد کنید.", reply_markup=get_cancel_kb())
    await state.update_data(pack_price=int(message.text))
    await delete_user_message(bot, message)
    await message.answer(
        "⏳ <b>مدت اعتبار بسته را به روز وارد کنید:</b>",
        parse_mode="HTML",
        reply_markup=get_cancel_kb()
    )
    await state.set_state(AdminFlow.wait_for_pack_days)

@dp.message(AdminFlow.wait_for_pack_days)
async def admin_add_pack_days(message: types.Message, state: FSMContext):
    if message.from_user.id not in get_admin_ids():
        return await state.clear()
    if not message.text.isdigit() or int(message.text) <= 0:
        return await message.answer("⚠️ لطفاً یک عدد مثبت وارد کنید.", reply_markup=get_cancel_kb())
    days = int(message.text)
    data = await state.get_data()
    editing_id = data.get('editing_pack_id')
    with SessionLocal() as db:
        if editing_id:
            pack = db.query(TrafficPack).filter(TrafficPack.id == editing_id).first()
            if pack:
                pack.name = data['pack_name']
                pack.traffic_gb = data['pack_gb']
                pack.price = data['pack_price']
                pack.duration_days = days
                db.commit()
                await message.answer(f"✅ <b>بسته '{pack.name}' با موفقیت ویرایش شد.</b>", reply_markup=get_admin_menu(), parse_mode="HTML")
            else:
                await message.answer("❌ بسته یافت نشد.", reply_markup=get_admin_menu())
        else:
            new_pack = TrafficPack(
                name=data['pack_name'],
                traffic_gb=data['pack_gb'],
                price=data['pack_price'],
                duration_days=days,
                is_active=True
            )
            db.add(new_pack)
            db.commit()
            await message.answer(f"✅ <b>بسته '{new_pack.name}' با موفقیت اضافه شد.</b>", reply_markup=get_admin_menu(), parse_mode="HTML")
    await state.clear()

@dp.callback_query(F.data.startswith("edit_pack_"))
async def admin_edit_pack_start(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in get_admin_ids():
        return await callback.answer("⛔ دسترسی غیرمجاز", show_alert=True)
    pack_id = int(callback.data.split("_")[2])
    with SessionLocal() as db:
        pack = db.query(TrafficPack).filter(TrafficPack.id == pack_id).first()
        if not pack:
            await callback.answer("❌ بسته یافت نشد.", show_alert=True)
            return
    await state.update_data(editing_pack_id=pack_id, pack_name=pack.name, pack_gb=pack.traffic_gb, pack_price=pack.price)
    await callback.message.edit_text(
        f"✏️ <b>ویرایش بسته</b>\n━━━━━━━━━━━━━━━━━━\n"
        f"نام فعلی: <b>{pack.name}</b>\n\n"
        f"نام جدید را وارد کنید (یا برای رد شدن از این مرحله دکمه زیر را بزنید):",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⏭ رد شدن (بدون تغییر)", callback_data="edit_pack_skip_name")]
        ]),
        parse_mode="HTML"
    )
    await state.set_state(AdminFlow.wait_for_pack_name)

@dp.callback_query(F.data == "edit_pack_skip_name")
async def admin_edit_pack_skip_name(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in get_admin_ids():
        return await callback.answer("⛔ دسترسی غیرمجاز", show_alert=True)
    await callback.answer()
    await callback.message.delete()
    data = await state.get_data()
    await callback.message.answer(
        f"📶 <b>حجم جدید (فعلی: {data['pack_gb']} GB) را وارد کنید یا رد شوید:</b>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⏭ رد شدن (بدون تغییر)", callback_data="edit_pack_skip_gb")]
        ]),
        parse_mode="HTML"
    )
    await state.set_state(AdminFlow.wait_for_pack_gb)

@dp.callback_query(F.data == "edit_pack_skip_gb")
async def admin_edit_pack_skip_gb(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in get_admin_ids():
        return await callback.answer("⛔ دسترسی غیرمجاز", show_alert=True)
    await callback.answer()
    await callback.message.delete()
    data = await state.get_data()
    await callback.message.answer(
        f"💰 <b>قیمت جدید (فعلی: {data['pack_price']:,} تومان) را وارد کنید یا رد شوید:</b>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⏭ رد شدن (بدون تغییر)", callback_data="edit_pack_skip_price")]
        ]),
        parse_mode="HTML"
    )
    await state.set_state(AdminFlow.wait_for_pack_price)

@dp.callback_query(F.data == "edit_pack_skip_price")
async def admin_edit_pack_skip_price(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in get_admin_ids():
        return await callback.answer("⛔ دسترسی غیرمجاز", show_alert=True)
    await callback.answer()
    await callback.message.delete()
    data = await state.get_data()
    await callback.message.answer(
        f"⏳ <b>مدت جدید (فعلی: {data.get('pack_days', 30)} روز) را وارد کنید یا رد شوید:</b>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⏭ رد شدن (بدون تغییر)", callback_data="edit_pack_skip_days")]
        ]),
        parse_mode="HTML"
    )
    await state.set_state(AdminFlow.wait_for_pack_days)

@dp.callback_query(F.data == "edit_pack_skip_days")
async def admin_edit_pack_skip_days(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in get_admin_ids():
        return await callback.answer("⛔ دسترسی غیرمجاز", show_alert=True)
    await callback.answer()
    # Finish editing without changes (just save current values)
    data = await state.get_data()
    pack_id = data.get('editing_pack_id')
    if pack_id:
        with SessionLocal() as db:
            pack = db.query(TrafficPack).filter(TrafficPack.id == pack_id).first()
            if pack:
                # No changes, just confirm
                await callback.message.delete()
                await callback.message.answer(f"✅ <b>بسته '{pack.name}' بدون تغییر ذخیره شد.</b>", reply_markup=get_admin_menu(), parse_mode="HTML")
                await state.clear()
                return
    await callback.message.answer("❌ بسته یافت نشد.", reply_markup=get_admin_menu())
    await state.clear()

# Override the generic message handlers for editing: they already handle the flow, but we need to handle the skip callbacks.
# The skip callbacks above set the state accordingly. For the message handlers, they already check if editing_pack_id exists.

@dp.callback_query(F.data.startswith("toggle_pack_"))
async def admin_toggle_pack(callback: types.CallbackQuery):
    if callback.from_user.id not in get_admin_ids():
        return await callback.answer("⛔ دسترسی غیرمجاز", show_alert=True)
    pack_id = int(callback.data.split("_")[2])
    with SessionLocal() as db:
        pack = db.query(TrafficPack).filter(TrafficPack.id == pack_id).first()
        if not pack:
            await callback.answer("❌ بسته یافت نشد.", show_alert=True)
            return
        pack.is_active = not pack.is_active
        db.commit()
    await callback.answer(f"✅ وضعیت بسته به {'فعال' if pack.is_active else 'غیرفعال'} تغییر یافت.", show_alert=True)
    # Refresh the list
    await admin_traffic_packs(callback, None)

@dp.callback_query(F.data.startswith("del_pack_"))
async def admin_delete_pack_confirm(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in get_admin_ids():
        return await callback.answer("⛔ دسترسی غیرمجاز", show_alert=True)
    pack_id = int(callback.data.split("_")[2])
    with SessionLocal() as db:
        pack = db.query(TrafficPack).filter(TrafficPack.id == pack_id).first()
        if not pack:
            await callback.answer("❌ بسته یافت نشد.", show_alert=True)
            return
    await state.update_data(pack_id=pack_id)
    await callback.message.edit_text(
        f"⚠️ <b>آیا از حذف بسته <code>{pack.name}</code> اطمینان دارید؟</b>\n\nاین عملیات غیرقابل بازگشت است.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🗑 بله، حذف کن", callback_data="confirm_delete_pack")],
            [InlineKeyboardButton(text="❌ انصراف", callback_data="admin_traffic_packs")]
        ]),
        parse_mode="HTML"
    )
    await state.set_state(AdminFlow.wait_for_pack_delete_confirm)

@dp.callback_query(F.data == "confirm_delete_pack")
async def admin_delete_pack_execute(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in get_admin_ids():
        return await callback.answer("⛔ دسترسی غیرمجاز", show_alert=True)
    data = await state.get_data()
    pack_id = data.get('pack_id')
    if not pack_id:
        await callback.answer("❌ شناسه بسته یافت نشد.", show_alert=True)
        return
    with SessionLocal() as db:
        pack = db.query(TrafficPack).filter(TrafficPack.id == pack_id).first()
        if not pack:
            await callback.answer("❌ بسته یافت نشد.", show_alert=True)
            return
        db.delete(pack)
        db.commit()
    await state.clear()
    await callback.answer("✅ بسته حذف شد.", show_alert=True)
    await admin_traffic_packs(callback, state)

@dp.callback_query(F.data == "admin_billing_report")
async def billing_report_menu(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in get_admin_ids(): return
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📅 امروز", callback_data="billing_today")],
        [InlineKeyboardButton(text="📆 این هفته", callback_data="billing_week")],
        [InlineKeyboardButton(text="📆 این ماه", callback_data="billing_month")],
        [InlineKeyboardButton(text="🗓 بازه دلخواه", callback_data="billing_custom")],
        [InlineKeyboardButton(text="⬅️ بازگشت", callback_data="admin_panel")]
    ])
    await callback.message.edit_text("📊 <b>گزارش فروش</b>\nبازه مورد نظر را انتخاب کنید:", reply_markup=kb, parse_mode="HTML")

@dp.callback_query(F.data == "admin_approved_receipts")
async def admin_approved_receipts(callback: types.CallbackQuery):
    if callback.from_user.id not in get_admin_ids():
        return
    with SessionLocal() as db:
        receipts = db.query(Invoice).filter(
            Invoice.status == "COMPLETE",
            Invoice.screenshot_local_path.isnot(None),
            Invoice.screenshot_local_path != ""
        ).order_by(Invoice.created_at.desc()).limit(50).all()
    if not receipts:
        return await callback.message.edit_text(
            "📋 <b>رسیدهای تایید شده</b>\n━━━━━━━━━━━━━━━━━━\n❌ هیچ رسید تایید شده‌ای یافت نشد.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ بازگشت", callback_data="admin_panel")]]),
            parse_mode="HTML"
        )
    text = "📋 <b>رسیدهای تایید شده (آخرین ۵۰)</b>\n━━━━━━━━━━━━━━━━━━\n"
    for inv in receipts:
        date_str = inv.created_at.strftime('%Y-%m-%d %H:%M')
        text += f"#{inv.id} | کاربر {inv.telegram_user_id} | {date_str}\n"
    text += "\n👇 برای مشاهده رسید، روی دکمه مربوطه کلیک کنید:"
    kb_buttons = []
    for inv in receipts:
        kb_buttons.append([InlineKeyboardButton(text=f"🧾 مشاهده رسید #{inv.id}", callback_data=f"view_receipt_{inv.id}")])
    kb_buttons.append([InlineKeyboardButton(text="⬅️ بازگشت", callback_data="admin_panel")])
    kb = InlineKeyboardMarkup(inline_keyboard=kb_buttons)
    await callback.message.edit_text(text, reply_markup=kb, parse_mode="HTML")

@dp.callback_query(F.data.startswith("view_receipt_"))
async def view_receipt(callback: types.CallbackQuery):
    if callback.from_user.id not in get_admin_ids():
        return
    invoice_id = int(callback.data.split("_")[2])
    with SessionLocal() as db:
        invoice = db.query(Invoice).filter(Invoice.id == invoice_id).first()
        if not invoice or not invoice.screenshot_local_path:
            await callback.answer("❌ رسید یافت نشد.", show_alert=True)
            return
        file_path = invoice.screenshot_local_path
    try:
        with open(file_path, 'rb') as f:
            photo_data = f.read()
        await bot.send_photo(
            callback.from_user.id,
            BufferedInputFile(photo_data, filename=f"receipt_{invoice_id}.jpg"),
            caption=f"🧾 <b>رسید فاکتور #{invoice_id}</b>",
            parse_mode="HTML"
        )
        await callback.answer("✅ رسید ارسال شد.")
    except FileNotFoundError:
        await callback.answer("❌ فایل رسید روی سرور موجود نیست.", show_alert=True)
    except Exception as e:
        await callback.answer(f"❌ خطا: {str(e)[:50]}", show_alert=True)

@dp.callback_query(F.data == "admin_custom_receipt")
async def admin_custom_receipt_start(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in get_admin_ids():
        return await callback.answer("⛔ دسترسی غیرمجاز", show_alert=True)
    await state.set_state(AdminFlow.wait_for_custom_receipt_target)
    await callback.message.edit_text(
        "📨 <b>صدور فاکتور سفارشی</b>\n\n"
        "شناسه عددی تلگرام یا نام سرویس (ایمیل کلاینت) کاربر را وارد کنید:\n"
        "▫️ مثال برای شناسه: <code>123456789</code>\n"
        "▫️ مثال برای نام سرویس: <code>user_01</code>",
        reply_markup=get_cancel_kb(),
        parse_mode="HTML"
    )

@dp.message(AdminFlow.wait_for_custom_receipt_target)
async def admin_custom_receipt_target(message: types.Message, state: FSMContext):
    if message.from_user.id not in get_admin_ids():
        return await state.clear()
    target = message.text.strip()
    user_id = None
    client_name = None
    if target.isdigit():
        uid = int(target)
        with SessionLocal() as db:
            inv = db.query(Invoice).filter(Invoice.telegram_user_id == uid).first()
            if inv:
                user_id = uid
                latest = db.query(Invoice).filter(Invoice.telegram_user_id == uid).order_by(Invoice.created_at.desc()).first()
                if latest and latest.client_name:
                    client_name = latest.client_name
            else:
                ref = db.query(ReferralCode).filter(ReferralCode.telegram_user_id == uid).first()
                if ref:
                    user_id = uid
                else:
                    trial = db.query(TrialUsage).filter(TrialUsage.telegram_user_id == uid).first()
                    if trial:
                        user_id = uid
    else:
        with SessionLocal() as db:
            inv = db.query(Invoice).filter(Invoice.client_name == target).order_by(Invoice.created_at.desc()).first()
            if inv:
                user_id = inv.telegram_user_id
                client_name = target
    if user_id is None:
        await message.answer(
            "❌ کاربری با این شناسه یا نام سرویس یافت نشد. لطفاً دوباره وارد کنید یا انصراف دهید.",
            reply_markup=get_cancel_kb()
        )
        return
    await state.update_data(target_user_id=user_id, target_client_name=client_name)
    await delete_user_message(bot, message)
    await message.answer(
        f"✅ کاربر پیدا شد: شناسه <code>{user_id}</code> {f'نام سرویس: <code>{client_name}</code>' if client_name else ''}\n\n"
        "💰 <b>مبلغ فاکتور را به تومان وارد کنید (فقط عدد):</b>",
        reply_markup=get_cancel_kb(),
        parse_mode="HTML"
    )
    await state.set_state(AdminFlow.wait_for_custom_receipt_amount)

@dp.message(AdminFlow.wait_for_custom_receipt_amount)
async def admin_custom_receipt_amount(message: types.Message, state: FSMContext):
    if message.from_user.id not in get_admin_ids():
        return await state.clear()
    if not message.text.isdigit() or int(message.text) <= 0:
        await message.answer("⚠️ لطفاً یک عدد مثبت معتبر وارد کنید.", reply_markup=get_cancel_kb())
        return
    amount = int(message.text)
    await state.update_data(receipt_amount=amount)
    await delete_user_message(bot, message)
    await message.answer(
        "📝 <b>توضیحات (اختیاری)</b>\n\n"
        "توضیحات مربوط به فاکتور را وارد کنید (مثلاً: ارتقاء ویژه، سفارش غیررسمی، ...).\n"
        "برای رد شدن، دکمه زیر را بزنید.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⏭ رد شدن", callback_data="skip_custom_receipt_desc")]
        ]),
        parse_mode="HTML"
    )
    await state.set_state(AdminFlow.wait_for_custom_receipt_description)

@dp.callback_query(F.data == "skip_custom_receipt_desc", AdminFlow.wait_for_custom_receipt_description)
async def skip_custom_receipt_desc(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in get_admin_ids():
        return await callback.answer("⛔ دسترسی غیرمجاز", show_alert=True)
    await state.update_data(receipt_description=None)
    await callback.answer()
    await callback.message.delete()
    await ask_receipt_photo(callback.message, state)

@dp.message(AdminFlow.wait_for_custom_receipt_description)
async def admin_custom_receipt_description(message: types.Message, state: FSMContext):
    if message.from_user.id not in get_admin_ids():
        return await state.clear()
    description = message.text.strip()
    await state.update_data(receipt_description=description)
    await delete_user_message(bot, message)
    await ask_receipt_photo(message, state)

async def ask_receipt_photo(msg: types.Message, state: FSMContext):
    """Send photo request and set state."""
    await msg.answer(
        "🖼 <b>عکس رسید (اختیاری)</b>\n\n"
        "در صورت تمایل، عکس رسید را ارسال کنید.\n"
        "برای رد شدن، دکمه زیر را بزنید.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⏭ رد شدن", callback_data="skip_custom_receipt_photo")]
        ]),
        parse_mode="HTML"
    )
    await state.set_state(AdminFlow.wait_for_custom_receipt_photo)

@dp.callback_query(F.data == "skip_custom_receipt_photo", AdminFlow.wait_for_custom_receipt_photo)
async def skip_custom_receipt_photo(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in get_admin_ids():
        return await callback.answer("⛔ دسترسی غیرمجاز", show_alert=True)
    await state.update_data(receipt_photo_path=None)
    await callback.answer()
    await callback.message.delete()
    await create_custom_invoice_and_send_request(callback.message, state)

@dp.message(AdminFlow.wait_for_custom_receipt_photo, F.photo)
async def admin_custom_receipt_photo(message: types.Message, state: FSMContext):
    if message.from_user.id not in get_admin_ids():
        return await state.clear()
    # Save photo
    photo = message.photo[-1]
    file_path = f"./storage/receipts/{message.from_user.id}_{time.time_ns()}.jpg"
    await bot.download(photo, destination=file_path)
    await state.update_data(receipt_photo_path=file_path)
    await delete_user_message(bot, message)
    await create_custom_invoice_and_send_request(message, state)

async def create_custom_invoice_and_send_request(msg: types.Message, state: FSMContext):
    """Create a PENDING invoice and send payment request to user."""
    data = await state.get_data()
    target_user_id = data.get('target_user_id')
    target_client_name = data.get('target_client_name')
    amount = data.get('receipt_amount')
    description = data.get('receipt_description')
    photo_path = data.get('receipt_photo_path')

    if not target_user_id or amount is None:
        await msg.answer("❌ اطلاعات ناقص است. لطفاً دوباره از ابتدا شروع کنید.", reply_markup=get_main_menu(msg.from_user.id))
        await state.clear()
        return

    # Create invoice
    with SessionLocal() as db:
        invoice = Invoice(
            telegram_user_id=target_user_id,
            total_price=amount,
            original_price=amount,
            discount_amount=0,
            client_name=target_client_name,
            action_type="CUSTOM_ORDER",
            status="PENDING",
            screenshot_local_path=photo_path,
            description=description
        )
        db.add(invoice)
        db.commit()
        db.refresh(invoice)
        invoice_id = invoice.id

    # Send payment request to user
    payment_text = (
        f"🧾 <b>یک فاکتور جدید برای شما صادر شده است</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"💰 مبلغ: <b>{amount:,} تومان</b>\n"
    )
    if description:
        payment_text += f"📝 توضیحات: {description}\n"
    payment_text += (
        f"\n🔹 برای پرداخت این فاکتور، روی دکمه زیر کلیک کنید و رسید پرداخت را ارسال کنید."
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💳 پرداخت فاکتور", callback_data=f"pay_custom_invoice_{invoice_id}")]
    ])
    try:
        await bot.send_message(target_user_id, payment_text, parse_mode="HTML", reply_markup=kb)
    except Exception:
        pass  # user might have blocked bot

    # Notify admin
    admin_text = (
        f"✅ <b>فاکتور سفارشی ایجاد شد</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"👤 کاربر: <code>{target_user_id}</code>\n"
        f"🔖 نام سرویس: <code>{target_client_name or 'نامشخص'}</code>\n"
        f"💰 مبلغ: <b>{amount:,} تومان</b>\n"
    )
    if description:
        admin_text += f"📝 توضیحات: {description}\n"
    admin_text += f"🆔 فاکتور: #{invoice_id}\n\n"
    admin_text += "⏳ در انتظار پرداخت کاربر."

    for admin_id in get_admin_ids():
        try:
            if photo_path and os.path.exists(photo_path):
                with open(photo_path, 'rb') as f:
                    photo_data = f.read()
                await bot.send_photo(
                    admin_id,
                    BufferedInputFile(photo_data, filename=f"invoice_{invoice_id}.jpg"),
                    caption=admin_text,
                    parse_mode="HTML"
                )
            else:
                await bot.send_message(admin_id, admin_text, parse_mode="HTML")
        except Exception:
            pass

    await state.clear()
    await msg.answer(
        f"✅ <b>فاکتور سفارشی با موفقیت ایجاد شد.</b>\n\n"
        f"فاکتور #{invoice_id} ثبت شد و درخواست پرداخت برای کاربر ارسال گردید.",
        reply_markup=get_admin_menu(),
        parse_mode="HTML"
    )

@dp.callback_query(F.data.startswith("pay_custom_invoice_"))
async def pay_custom_invoice(callback: types.CallbackQuery, state: FSMContext):
    invoice_id = int(callback.data.split("_")[3])
    with SessionLocal() as db:
        invoice = db.query(Invoice).filter(Invoice.id == invoice_id).first()
        if not invoice:
            await callback.answer("❌ فاکتور یافت نشد.", show_alert=True)
            return
        if invoice.telegram_user_id != callback.from_user.id:
            await callback.answer("⛔ این فاکتور متعلق به شما نیست.", show_alert=True)
            return
        if invoice.status != "PENDING":
            await callback.answer(f"⚠️ وضعیت فاکتور: {invoice.status} — قابل پرداخت نیست.", show_alert=True)
            return
    await state.update_data(invoice_id=invoice_id)
    await callback.message.delete()
    await callback.message.answer(
        "💳 <b>پرداخت فاکتور</b>\n\n"
        "لطفاً رسید پرداخت را به صورت عکس ارسال کنید.",
        reply_markup=get_cancel_kb(),
        parse_mode="HTML"
    )
    await state.set_state(CustomPayFlow.wait_for_receipt)
    await callback.answer()

@dp.message(CustomPayFlow.wait_for_receipt, F.photo)
async def custom_pay_receipt(message: types.Message, state: FSMContext):
    data = await state.get_data()
    invoice_id = data.get('invoice_id')
    if not invoice_id:
        await message.answer("❌ خطا: شناسه فاکتور یافت نشد. لطفاً دوباره شروع کنید.", reply_markup=get_main_menu(message.from_user.id))
        await state.clear()
        return
    max_size_mb = 10
    with SessionLocal() as db:
        size_setting = db.query(AppSetting).filter(AppSetting.key == 'max_receipt_size_mb').first()
        if size_setting:
            max_size_mb = int(size_setting.value)
    max_bytes = max_size_mb * 1024 * 1024
    if message.photo[-1].file_size > max_bytes:
        await message.answer(f"⚠️ حجم عکس بیش از حد مجاز ({max_size_mb} MB) است. لطفاً عکس را فشرده کنید و دوباره ارسال نمایید.", reply_markup=get_cancel_kb())
        return
    photo = message.photo[-1]
    file_path = f"./storage/receipts/{message.from_user.id}_{time.time_ns()}.jpg"
    await bot.download(photo, destination=file_path)
    with SessionLocal() as db:
        invoice = db.query(Invoice).filter(Invoice.id == invoice_id).first()
        if not invoice:
            await message.answer("❌ فاکتور یافت نشد.", reply_markup=get_main_menu(message.from_user.id))
            await state.clear()
            return
        invoice.screenshot_local_path = file_path
        invoice.status = "PENDING"
        db.commit()
        buyer_name = message.from_user.full_name or message.from_user.first_name or str(message.from_user.id)
        buyer_username = f"@{message.from_user.username}" if message.from_user.username else "ندارد"
        amount = invoice.total_price
        description = invoice.description
        client_name = invoice.client_name or "نامشخص"
    admin_kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="✅ تایید", callback_data=f"approve_{invoice_id}", style="success"),
        InlineKeyboardButton(text="⛔ رد کردن", callback_data=f"reject_{invoice_id}", style="danger")
    ]])
    caption = f"🧾 <b>فاکتور سفارشی #{invoice_id}</b>\n━━━━━━━━━━━━━━━━━━\n👤 خریدار: <b>{buyer_name}</b>\n📛 یوزرنیم: <code>{buyer_username}</code>\n🆔 شناسه: <code>{message.from_user.id}</code>"
    if client_name:
        caption += f"\n🔖 نام سرویس: <code>{client_name}</code>"
    caption += f"\n💰 مبلغ: <b>{amount:,} تومان</b>"
    if description:
        caption += f"\n📝 توضیحات: {description}"
    for admin_id in get_admin_ids():
        try:
            await bot.send_photo(admin_id, photo.file_id, caption=caption, reply_markup=admin_kb, parse_mode="HTML")
        except Exception:
            pass
    await message.answer(
        "✅ <b>رسید شما با موفقیت ثبت شد!</b> 🙏\n\n"
        "مدیران سیستم رسید شما را بررسی می‌کنند.\n"
        "پس از تایید، به شما اطلاع داده می‌شود.",
        reply_markup=get_main_menu(message.from_user.id),
        parse_mode="HTML"
    )
    await state.clear()

@dp.callback_query(F.data.startswith("billing_"))
async def billing_generate(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in get_admin_ids(): return
    now = datetime.now(timezone.utc)
    if callback.data == "billing_today":
        start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        end = now
    elif callback.data == "billing_week":
        start = now - timedelta(days=now.weekday())
        start = start.replace(hour=0, minute=0, second=0, microsecond=0)
        end = now
    elif callback.data == "billing_month":
        start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        end = now
    elif callback.data == "billing_custom":
        await callback.message.edit_text("📅 <b>تاریخ شروع را به فرمت YYYY-MM-DD وارد کنید:</b>", reply_markup=get_cancel_kb(), parse_mode="HTML")
        await state.set_state(AdminFlow.wait_for_billing_date_range)
        return
    else:
        return
    await send_billing_report(callback.message, start, end)

@dp.message(AdminFlow.wait_for_billing_date_range)
async def billing_custom_handler(message: types.Message, state: FSMContext):
    data = await state.get_data()
    if 'billing_start' not in data:
        try:
            start = datetime.strptime(message.text.strip(), '%Y-%m-%d').replace(tzinfo=timezone.utc)
        except ValueError:
            return await message.answer("⚠️ فرمت نامعتبر. لطفاً به فرمت YYYY-MM-DD وارد کنید.", reply_markup=get_cancel_kb())
        await state.update_data(billing_start=start)
        await message.answer("📅 <b>تاریخ پایان را به فرمت YYYY-MM-DD وارد کنید:</b>", reply_markup=get_cancel_kb(), parse_mode="HTML")
    else:
        try:
            end = (datetime.strptime(message.text.strip(), '%Y-%m-%d') + timedelta(days=1) - timedelta(seconds=1)).replace(tzinfo=timezone.utc)
        except ValueError:
            return await message.answer("⚠️ فرمت نامعتبر. لطفاً به فرمت YYYY-MM-DD وارد کنید.", reply_markup=get_cancel_kb())
        start = data['billing_start']
        await state.clear()
        await send_billing_report(message, start, end)

async def send_billing_report(message: types.Message, start: datetime, end: datetime):
    with SessionLocal() as db:
        invoices = db.query(Invoice).filter(
            Invoice.status == "COMPLETE",
            Invoice.created_at >= start,
            Invoice.created_at <= end
        ).all()
        # Prefetch all plans for breakdown
        all_plans = {p.id: p.name for p in db.query(Plan).all()}
    
    total_revenue = sum(inv.total_price or 0 for inv in invoices)
    total_transactions = len(invoices)
    total_discount = sum(inv.discount_amount or 0 for inv in invoices)
    breakdown_action = {}
    breakdown_plan = {}
    action_labels = {"NEW": "🆕 خرید جدید", "RENEW": "🔄 تمدید", "TOPUP": "➕ حجم اضافه", "TRIAL": "🎁 تست رایگان", "REFERRAL_REWARD": "🤝 پاداش معرفی", "RESELLER_NEW": "🧑‍💼 نمایندگی (جدید)", "RESELLER_RENEW": "🧑‍💼 نمایندگی (تمدید)", "RESELLER_TOPUP": "🧑‍💼 نمایندگی (حجم)", "RESELLER_PACK_BUY": "📦 خرید بسته ترافیک"}
    for inv in invoices:
        action_label = action_labels.get(inv.action_type, inv.action_type)
        breakdown_action[action_label] = breakdown_action.get(action_label, 0) + 1
        if inv.plan_id:
            plan_name = all_plans.get(inv.plan_id, f"ID {inv.plan_id}")
            breakdown_plan[plan_name] = breakdown_plan.get(plan_name, 0) + 1
    
    text = (
        f"📊 <b>گزارش فروش</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"📅 <b>بازه:</b> {start.strftime('%Y-%m-%d')} تا {end.strftime('%Y-%m-%d')}\n"
        f"\n"
        f"💰 <b>درآمد کل:</b> {total_revenue:,} تومان\n"
        f"🧾 <b>تعداد تراکنش‌ها:</b> {total_transactions}\n"
        f"🎫 <b>مجموع تخفیف:</b> {total_discount:,} تومان\n"
        f"\n"
        f"📂 <b>تفکیک بر اساس نوع:</b>\n"
    )
    for action, count in breakdown_action.items():
        text += f"   {action}: <b>{count}</b>\n"
    if breakdown_plan:
        text += f"\n📦 <b>تفکیک بر اساس پلن:</b>\n"
        for plan, count in breakdown_plan.items():
            text += f"   ▪️ {plan}: <b>{count}</b>\n"
    
    text += f"\n━━━━━━━━━━━━━━━━━━\n📥 <i>برای دانلود جزئیات کامل به صورت CSV از دکمه زیر استفاده کنید.</i>"
    
    csv_lines = ["شناسه,نوع,مبلغ,تخفیف,تاریخ"]
    for inv in invoices:
        csv_lines.append(f"{inv.id},{inv.action_type},{inv.total_price},{inv.discount_amount or 0},{inv.created_at.strftime('%Y-%m-%d %H:%M')}")
    csv_data = "\n".join(csv_lines)
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📥 دانلود CSV", callback_data="billing_export")],
        [InlineKeyboardButton(text="⬅️ بازگشت به گزارش", callback_data="admin_billing_report")]
    ])
    await message.answer(text, parse_mode="HTML", reply_markup=kb)
    await redis_client.set(f"billing_export_{message.from_user.id}", csv_data, ex=1800)

@dp.callback_query(F.data == "billing_export")
async def billing_export(callback: types.CallbackQuery):
    csv_data = await redis_client.get(f"billing_export_{callback.from_user.id}")
    if not csv_data:
        return await callback.answer("گزارشی برای صادرات وجود ندارد. لطفاً دوباره درخواست دهید.", show_alert=True)
    file = BufferedInputFile(csv_data.encode('utf-8'), filename="billing_report.csv")
    await callback.message.answer_document(file, caption="📊 گزارش فروش")
    try:
        await callback.answer()
    except Exception:
        pass

# ==============================================================================
# ADMIN: PANEL TRAFFIC (daily usage)
# ==============================================================================
@dp.callback_query(F.data == "admin_panel_traffic")
async def admin_panel_traffic(callback: types.CallbackQuery):
    if callback.from_user.id not in get_admin_ids():
        return await callback.answer("⛔ دسترسی غیرمجاز", show_alert=True)
    with SessionLocal() as db:
        records = db.query(PanelTraffic).order_by(PanelTraffic.date.desc()).limit(31).all()  # last 31 days
    if not records:
        return await callback.message.edit_text(
            "📊 <b>مصرف روزانه پنل</b>\n━━━━━━━━━━━━━━━━━━\n❌ هنوز داده‌ای جمع‌آوری نشده است.\n\n"
            "دستگاه جمع‌آوری خودکار هر شب ساعت ۲۳:۵۵ اجرا می‌شود.\n"
            "برای جمع‌آوری دستی، از دکمه «🔄 جمع‌آوری دستی» استفاده کنید.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔄 جمع‌آوری دستی", callback_data="admin_force_traffic")],
                [InlineKeyboardButton(text="⬅️ بازگشت به پنل", callback_data="admin_panel")]
            ]),
            parse_mode="HTML"
        )
    # Show most recent first, we'll reverse for chronological display
    records = records[::-1]  # oldest first
    text = "📊 <b>مصرف روزانه پنل (آخرین ۳۰ روز)</b>\n━━━━━━━━━━━━━━━━━━\n"
    for rec in records:
        date_str = rec.date.strftime('%Y-%m-%d')
        up = format_size(rec.daily_up)
        down = format_size(rec.daily_down)
        total = format_size(rec.daily_up + rec.daily_down)
        text += f"📅 {date_str}  |  ⬆ {up}  ⬇ {down}  |  کل: {total}\n"
    # Add a summary line for total cumulative
    total_up = sum(r.daily_up for r in records)
    total_down = sum(r.daily_down for r in records)
    text += f"\n━━━━━━━━━━━━━━━━━━\n📦 <b>مجموع {len(records)} روز:</b> ⬆ {format_size(total_up)}  ⬇ {format_size(total_down)}"
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 جمع‌آوری دستی", callback_data="admin_force_traffic")],
        [InlineKeyboardButton(text="⬅️ بازگشت به پنل", callback_data="admin_panel")]
    ])
    await callback.message.edit_text(text, reply_markup=kb, parse_mode="HTML")

@dp.callback_query(F.data == "admin_force_traffic")
async def admin_force_traffic(callback: types.CallbackQuery):
    if callback.from_user.id not in get_admin_ids():
        return await callback.answer("⛔ دسترسی غیرمجاز", show_alert=True)
    await callback.message.edit_text(
        "⏳ <b>در حال جمع‌آوری مصرف روزانه...</b>\n\nاین عملیات ممکن است چند ثانیه طول بکشد.",
        parse_mode="HTML"
    )
    try:
        tasks.collect_panel_traffic.delay()
        await callback.message.edit_text(
            "✅ <b>دستور جمع‌آوری مصرف روزانه با موفقیت صادر شد.</b>\n\n"
            "نتیجه در چند لحظه در دیتابیس ثبت می‌شود. برای مشاهده، دکمه «📊 مصرف روزانه پنل» را بزنید.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="📊 مشاهده مصرف", callback_data="admin_panel_traffic")],
                [InlineKeyboardButton(text="⬅️ بازگشت به پنل", callback_data="admin_panel")]
            ]),
            parse_mode="HTML"
        )
    except Exception as e:
        await callback.message.edit_text(
            f"❌ <b>خطا در صدور دستور:</b>\n<code>{str(e)[:200]}</code>",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ بازگشت", callback_data="admin_panel")]]),
            parse_mode="HTML"
        )

# ==============================================================================
# USER LOGS: PURCHASE HISTORY & DELETED SERVICES
# ==============================================================================
@dp.callback_query(F.data == "user_logs")
async def user_logs_callback(callback: types.CallbackQuery):
    """Show user's purchase history including deleted services."""
    user_id = callback.from_user.id
    
    with SessionLocal() as db:
        # Get all invoices for this user (COMPLETE, DELETED, FAILED)
        invoices = db.query(Invoice).filter(
            Invoice.telegram_user_id == user_id,
            Invoice.status.in_(["COMPLETE", "DELETED", "FAILED"])
        ).order_by(Invoice.created_at.desc()).all()
        
        if not invoices:
            return await callback.message.edit_text(
                "📜 <b>تاریخچه خرید</b>\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"❌ هنوز هیچ خریدی ثبت نشده است.\n\n"
                f"برای مشاهده پلن‌ها و خرید سرویس جدید اقدام کنید.",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="🛒 خرید اشتراک", callback_data="buy_plan")],
                    [InlineKeyboardButton(text="⬅️ بازگشت", callback_data="my_plans")]
                ]),
                parse_mode="HTML"
            )
        
        text = "📜 <b>تاریخچه خرید و سرویس‌ها</b>\n"
        text += f"━━━━━━━━━━━━━━━━━━\n"
        text += f"👤 کاربر: {callback.from_user.first_name}\n\n"
        
        successful_purchases = []
        deleted_services = []
        
        for inv in invoices:
            invoice_info = {
                'id': inv.id,
                'created_at': inv.created_at,
                'client_name': inv.client_name,
                'status': inv.status,
                'action_type': inv.action_type,
                'total_price': inv.total_price or 0,
                'description': inv.description
            }
            
            if inv.status == "DELETED":
                deleted_services.append(invoice_info)
            elif inv.status == "COMPLETE" and inv.total_price and inv.total_price > 0:
                successful_purchases.append(invoice_info)
        
        # Show successful purchases with invoice numbers
        if successful_purchases:
            text += f"✅ <b>خریدهای موفق ({len(successful_purchases)} مورد)</b>\n"
            text += "<blockquote expandable>"
            for i, purchase in enumerate(successful_purchases[:10], 1):  # Limit to 10 most recent
                date_str = purchase['created_at'].strftime('%Y/%m/%d') if purchase['created_at'] else 'نامشخص'
                service_name = purchase['client_name'] or 'سرویس'
                price = f"{purchase['total_price']:,}"
                invoice_num = f"#{purchase['id']}"

                text += f"{i}. 📦 {service_name}\n"
                text += f"   📅 تاریخ: {date_str} | 💰 مبلغ: {price} تومان\n"
                text += f"   🧾 شماره فاکتور: {invoice_num}\n\n"

            if len(successful_purchases) > 10:
                text += f"... و {len(successful_purchases) - 10} مورد دیگر\n"
            text += "</blockquote>\n"

        # Show deleted services
        if deleted_services:
            text += f"❌ <b>سرویس‌های حذف‌شده ({len(deleted_services)} مورد)</b>\n"
            text += "<blockquote expandable>"
            for i, deleted in enumerate(deleted_services[:5], 1):  # Limit to 5 most recent
                date_str = deleted['created_at'].strftime('%Y/%m/%d') if deleted['created_at'] else 'نامشخص'
                service_name = deleted['client_name'] or 'سرویس'

                text += f"{i}. 🗑 {service_name}\n"
                text += f"   📅 تاریخ ایجاد: {date_str}\n"
                text += f"   ⚠️ به دلیل عدم تمدید حذف شده است\n\n"

            if len(deleted_services) > 5:
                text += f"... و {len(deleted_services) - 5} مورد دیگر\n"
            text += "</blockquote>"
        
        if not successful_purchases and not deleted_services:
            text += "ℹ️ هنوز خرید موفقی ثبت نشده است.\n"
        
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔄 بروزرسانی", callback_data="user_logs")],
            [InlineKeyboardButton(text="⬅️ بازگشت به سرویس‌ها", callback_data="my_plans")],
            [InlineKeyboardButton(text="🏠 منوی اصلی", callback_data="main_menu")]
        ])
        
        # Try to edit the message, if it fails send a new one
        try:
            await callback.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
        except Exception:
            # If message can't be edited (e.g., it's a new message or old message), send as new message
            await callback.message.answer(text, reply_markup=kb, parse_mode="HTML")


# ==============================================================================
# MAIN
# ==============================================================================
async def setup_bot_commands(bot: Bot):
    await bot.set_my_commands([
        BotCommand(command="start", description="شروع کار و مشاهده منوی اصلی"),
        BotCommand(command="menu", description="باز کردن منوی اصلی"),
    ])

async def heartbeat_writer():
    """Dead-man's switch: touch a Redis key every minute (TTL 5 min).

    The worker's ``system_heartbeat`` beat task reads this key; if it goes
    stale the bot process is presumed dead and admins are alerted — even
    though the worker itself is still perfectly alive.
    """
    while True:
        try:
            await redis_client.set("bot:heartbeat",
                                   datetime.now(timezone.utc).isoformat(), ex=300)
        except Exception as e:
            logger.warning(f"heartbeat write failed: {e}")
        await asyncio.sleep(60)

async def main():
    await setup_bot_commands(bot)
    asyncio.create_task(heartbeat_writer(), name="heartbeat_writer")
    await dp.start_polling(bot, skip_updates=True)

if __name__ == '__main__':
    asyncio.run(main())
