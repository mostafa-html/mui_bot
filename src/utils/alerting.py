"""
Rate-limited admin alerting helpers, shared by the bot process and the
Celery worker. Pure logic — no I/O here so both sides can plug in their own
send mechanism (aiogram bot vs raw HTTP).

Design: alerts are keyed by a signature (e.g. exception type + message).
The first occurrence within ``cooldown_seconds`` goes out immediately;
subsequent duplicates are suppressed and counted. When the cooldown expires
and duplicates were suppressed, the next alert mentions how many were eaten,
so bursts surface as one message instead of fifty.
"""
import time

# key -> (last_sent_monotonic, suppressed_count)
_recent: dict = {}

# Errors that are normal Telegram chatter, never worth paging an admin.
NOISE_MARKERS = (
    "message is not modified",
    "message to delete not found",
    "message can't be deleted",
    "query is too old",
    "query cannot be parsed",
    "message thread not found",
)


def should_alert(key: str, cooldown_seconds: int = 600) -> tuple:
    """Return (alert_now, suppressed_so_far) for this signature."""
    now = time.monotonic()
    last_ts, suppressed = _recent.get(key, (0.0, 0))
    if now - last_ts >= cooldown_seconds:
        _recent[key] = (now, 0)
        return True, suppressed
    _recent[key] = (last_ts, suppressed + 1)
    return False, suppressed


def is_noise(exception: Exception) -> bool:
    """True for routine Telegram API complaints that should never alert."""
    text = str(exception).lower()
    return any(marker in text for marker in NOISE_MARKERS)


def format_alert(source: str, exc: BaseException, context: str = None,
                 suppressed: int = 0) -> str:
    """Build one HTML admin alert message."""
    lines = [
        f"🚨 <b>خطا در {source}</b>",
        f"▫️ نوع: <code>{type(exc).__name__}</code>",
        f"▫️ پیام: <code>{str(exc)[:300]}</code>",
    ]
    if context:
        lines.append(f"▫️ زمینه: {context}")
    if suppressed:
        lines.append(f"🔇 {suppressed} مورد مشابه در فاصله کوتاه بی‌صدا شد.")
    return "\n".join(lines)
