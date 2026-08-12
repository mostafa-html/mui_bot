import time
from typing import Optional


def format_size(bytes_val: float) -> str:
    """Convert bytes to human-readable format."""
    if bytes_val <= 0:
        return "0 B"
    units = ['B', 'KB', 'MB', 'GB', 'TB']
    i = 0
    val = float(bytes_val)
    while val >= 1024 and i < len(units) - 1:
        val /= 1024
        i += 1
    if i == 0:
        if val < 1:
            return "<1 B"
        return f"{int(val)} {units[i]}"
    return f"{val:.1f} {units[i]}"


def format_price(price: int) -> str:
    """Format price with thousands separator and Toman suffix."""
    return f"{price:,} تومان"


def get_progress_bar(used: float, total: float, width: int = 10) -> str:
    """Generate a visual progress bar."""
    if total <= 0:
        return '♾️'
    ratio = min(used / total, 1.0)
    filled = int(ratio * width)
    empty = width - filled
    bar = '🟩' * filled + '⬜' * empty
    pct = int(ratio * 100)
    return f"{bar} {pct}%"


def format_expiry_remaining(expiry_ms: int) -> str:
    """Show remaining time until expiry in Persian."""
    if expiry_ms <= 0:
        return "♾️ نامحدود"
    remaining_seconds = max(0, expiry_ms - int(time.time() * 1000)) // 1000
    days = remaining_seconds // 86400
    hours = (remaining_seconds % 86400) // 3600
    minutes = (remaining_seconds % 3600) // 60
    if days > 30:
        months = days // 30
        remaining_days = days % 30
        return f"📅 {months} ماه و {remaining_days} روز"
    elif days > 0:
        return f"📅 {days} روز و {hours} ساعت"
    elif hours > 0:
        return f"📅 {hours} ساعت و {minutes} دقیقه"
    elif minutes > 0:
        return f"📅 {minutes} دقیقه"
    else:
        return "⚠️ کمتر از یک دقیقه"