from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton

from ..config import get_admin_ids
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
    if user_id in get_admin_ids():
        buttons.append([KeyboardButton(text="⚙️ پنل مدیریت")])
    if is_reseller(user_id):
        buttons.append([KeyboardButton(text="🧑‍💼 پنل نمایندگی")])
    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)


def get_admin_menu():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📊 داشبورد", callback_data="admin_dashboard")],
            [InlineKeyboardButton(text="🎯 انتخاب سرور اصلی", callback_data="admin_select_inbound")],
            [InlineKeyboardButton(text="🖥 وضعیت سرور", callback_data="admin_sys_status")],
            [
                InlineKeyboardButton(text="👤 مدیریت کاربر", callback_data="admin_manage_user"),
                InlineKeyboardButton(text="🗑 پاکسازی منقضی‌ها", callback_data="admin_del_depleted"),
            ],
            [
                InlineKeyboardButton(text="💾 بکاپ به تلگرام", callback_data="admin_backup_tg"),
                InlineKeyboardButton(text="🔄 پنیک/ری‌استارت", callback_data="admin_restart_menu"),
            ],
            [
                InlineKeyboardButton(text="➕ افزودن پلن", callback_data="admin_add_plan"),
                InlineKeyboardButton(text="📋 لیست پلن‌ها", callback_data="admin_view_plans"),
            ],
            [InlineKeyboardButton(text="🎫 کدهای تخفیف", callback_data="admin_coupon_menu")],
            [InlineKeyboardButton(text="🎁 تنظیمات تست رایگان", callback_data="admin_trial_settings")],
            [InlineKeyboardButton(text="🤝 تنظیمات معرفی", callback_data="admin_referral_settings")],
            [InlineKeyboardButton(text="🧑‍💼 مدیریت نمایندگان", callback_data="admin_reseller_menu")],
            [InlineKeyboardButton(text="📦 مدیریت بسته‌های ترافیک", callback_data="admin_traffic_packs")],
            [InlineKeyboardButton(text="📊 گزارش فروش", callback_data="admin_billing_report")],
            [InlineKeyboardButton(text="📨 صدور رسید سفارشی", callback_data="admin_custom_receipt")],
            [InlineKeyboardButton(text="📋 رسیدهای تایید شده", callback_data="admin_approved_receipts")],
            [InlineKeyboardButton(text="📢 پیام همگانی", callback_data="admin_broadcast")],
            [
                InlineKeyboardButton(text="💳 کارت پرداخت", callback_data="admin_set_card"),
                InlineKeyboardButton(text="🔗 لینک ساب", callback_data="admin_set_sub_link"),
                InlineKeyboardButton(text="👤 حساب پشتیبانی", callback_data="admin_set_support"),
            ],
            [
                InlineKeyboardButton(text="🔄 بازبینی فاکتورها", callback_data="admin_retry_invoices"),
            ],
            [
                InlineKeyboardButton(text="🔧 تعمیر سرویس‌های نامرئی", callback_data="admin_reconcile_names"),
                InlineKeyboardButton(text="👤 تعمیر یک کاربر", callback_data="admin_reconcile_user"),
            ],
            [
                InlineKeyboardButton(text="📊 مصرف روزانه پنل", callback_data="admin_panel_traffic"),
                InlineKeyboardButton(text="🔄 جمع‌آوری دستی", callback_data="admin_force_traffic")
            ],
            [InlineKeyboardButton(text="🏠 منوی اصلی", callback_data="main_menu")]
        ]
    )


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