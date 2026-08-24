from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton

from ..config import get_admin_ids, amnezia_visible
from ..services.reseller import is_reseller


JOIN_PROMPT_TEXT = (
    "🔐 <b>دسترسی نیاز به عضویت دارد</b>\n\n"
    "🙏 لطفاً ابتدا در <b>کانال رسمی</b> ما عضو شوید، سپس روی دکمه <b>«✅ عضو شدم»</b> بزنید.\n\n"
    "🔹 بعد از عضویت، ربات به صورت خودکار شما را شناسایی خواهد کرد.\n"
    "🌟 با تشکر از همراهی شما!"
)


def get_cancel_kb():
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="❌ انصراف", callback_data="cancel", style="danger")]]
    )


def get_back_kb(callback_data: str = "main_menu"):
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="⬅️ بازگشت", callback_data=callback_data)]]
    )


def get_main_menu(user_id: int):
    """Main menu as reply keyboard (buttons under text box)."""
    buttons = [
        [KeyboardButton(text="🛒 خرید اشتراک جدید")],
        [
            KeyboardButton(text="📦 سرویس‌های من"),
            KeyboardButton(text="🎁 تست رایگان"),
        ],
        [
            KeyboardButton(text="📜 تاریخچه خرید"),
            KeyboardButton(text="🤝 دعوت از دوستان"),
        ],
        [
            KeyboardButton(text="🎧 پشتیبانی"),
        ],
    ]
    if amnezia_visible(user_id):
        buttons.insert(1, [KeyboardButton(text="🟣 خرید Amnezia")])
    if user_id in get_admin_ids():
        buttons.append([KeyboardButton(text="⚙️ پنل مدیریت")])
    if is_reseller(user_id):
        buttons.append([KeyboardButton(text="🧑‍💼 پنل نمایندگی")])
    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)


def get_admin_menu():
    """Top-level admin panel: category launcher buttons only.

    Every actual action lives one level deeper (see ``get_admin_category_kb``)
    with its callback unchanged, so reorganizing never touches handlers.
    """
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📊 داشبورد و وضعیت", callback_data="admcat_dash")],
            [InlineKeyboardButton(text="🛍 پلن‌ها و فروش", callback_data="admcat_plans")],
            [InlineKeyboardButton(text="🧾 فاکتور و پرداخت", callback_data="admcat_invoices")],
            [InlineKeyboardButton(text="👥 کاربران و نمایندگان", callback_data="admcat_users")],
            [
                InlineKeyboardButton(text="🟣 Amnezia", callback_data="admcat_amnezia"),
                InlineKeyboardButton(text="🌐 تنظیمات", callback_data="admcat_settings"),
            ],
            [InlineKeyboardButton(text="🔧 تعمیر و نگهداری", callback_data="admcat_maint")],
            [InlineKeyboardButton(text="🏠 منوی اصلی", callback_data="main_menu")]
        ]
    )


# Category -> (title, [(text, callback), ...]) — callbacks MUST stay identical
# to the pre-reorganization ones; only the grouping is new.
ADMIN_CATEGORIES = {
    "admcat_dash": ("📊 داشبورد و وضعیت", [
        ("📊 داشبورد", "admin_dashboard"),
        ("🖥 وضعیت سرور", "admin_sys_status"),
        ("🎯 انتخاب سرور اصلی", "admin_select_inbound"),
        ("📊 مصرف روزانه پنل", "admin_panel_traffic"),
        ("🔄 جمع‌آوری دستی ترافیک", "admin_force_traffic"),
    ]),
    "admcat_plans": ("🛍 پلن‌ها و فروش", [
        ("➕ افزودن پلن", "admin_add_plan"),
        ("🟣 افزودن پلن Amnezia", "admin_add_plan_amz"),
        ("📋 لیست پلن‌ها", "admin_view_plans"),
        ("📦 مدیریت بسته‌های ترافیک", "admin_traffic_packs"),
        ("🎫 کدهای تخفیف", "admin_coupon_menu"),
        ("🎁 تنظیمات تست رایگان", "admin_trial_settings"),
        ("🤝 تنظیمات معرفی", "admin_referral_settings"),
    ]),
    "admcat_invoices": ("🧾 فاکتور و پرداخت", [
        ("🔄 بازبینی فاکتورها", "admin_retry_invoices"),
        ("📋 رسیدهای تایید شده", "admin_approved_receipts"),
        ("📨 صدور رسید سفارشی", "admin_custom_receipt"),
        ("📊 گزارش فروش", "admin_billing_report"),
        ("💳 کارت پرداخت", "admin_set_card"),
    ]),
    "admcat_users": ("👥 کاربران و نمایندگان", [
        ("👤 مدیریت کاربر", "admin_manage_user"),
        ("🗑 پاکسازی منقضی‌ها", "admin_del_depleted"),
        ("🧑‍💼 مدیریت نمایندگان", "admin_reseller_menu"),
        ("📢 پیام همگانی", "admin_broadcast"),
    ]),
    "admcat_amnezia": ("🟣 Amnezia", [
        ("📦 سرویس‌های Amnezia", "admin_amnezia"),
        ("➕ افزودن پلن Amnezia", "admin_add_plan_amz"),
        ("♻️ ریست تریال همه کاربران", "admztrial_reset"),
    ]),
    "admcat_maint": ("🔧 تعمیر و نگهداری", [
        ("🔧 تعمیر سرویس‌های نامرئی", "admin_reconcile_names"),
        ("👤 تعمیر یک کاربر", "admin_reconcile_user"),
        ("📥 همگام‌سازی گروه پنل", "admin_sync_group"),
        ("🔄 بررسی اینباندهای VLESS", "admin_vless_audit"),
        ("💾 بکاپ به تلگرام", "admin_backup_tg"),
        ("🔄 پنیک/ری‌استارت", "admin_restart_menu"),
    ]),
    "admcat_settings": ("🌐 تنظیمات", [
        ("🔗 لینک ساب", "admin_set_sub_link"),
        ("👤 حساب پشتیبانی", "admin_set_support"),
        ("💳 کارت پرداخت", "admin_set_card"),
    ]),
}


def get_admin_category_kb(callback_data: str):
    """Submenu keyboard for one admin category; unknown ids fall back to the
    top-level admin panel so a stale button can never dead-end."""
    title, items = ADMIN_CATEGORIES.get(
        callback_data,
        ADMIN_CATEGORIES["admcat_dash"])
    rows = [[InlineKeyboardButton(text=text, callback_data=cb)] for text, cb in items]
    rows.append([InlineKeyboardButton(text="⬅️ پنل مدیریت", callback_data="admin_panel")])
    return title, InlineKeyboardMarkup(inline_keyboard=rows)


def get_reseller_menu():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="➕ ساخت سرویس جدید", callback_data="res_new")],
            [InlineKeyboardButton(text="📦 سرویس‌های من", callback_data="res_list")],
            [InlineKeyboardButton(text="📊 موجودی ترافیک", callback_data="res_balance")],
            [InlineKeyboardButton(text="🛒 خرید بسته ترافیک", callback_data="res_buy_pack")],
            [InlineKeyboardButton(text="🏠 منوی اصلی", callback_data="main_menu")]
        ]
    )


def get_join_prompt_kb(req_channel_link: str):
    """Join-channel prompt with an inline 'I joined' re-check button."""
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🔐 عضویت در کانال رسمی", url=req_channel_link)],
            [InlineKeyboardButton(text="✅ عضو شدم", callback_data="check_join")]
        ]
    )
