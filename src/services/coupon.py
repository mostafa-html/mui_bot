import logging
from datetime import datetime, timezone
from typing import Optional, Tuple

from database import SessionLocal, Coupon, CouponUsage, AppSetting


def validate_coupon(code: str, user_id: int, action_type: str, db_session=None) -> Tuple[bool, Optional[Coupon], Optional[str]]:
    """
    Validate a coupon code for a specific user and action type.
    
    Returns:
        (valid, coupon, error_message)
        - valid: True if coupon is valid
        - coupon: Coupon object if valid, else None
        - error_message: error description if invalid, else None
    """
    def _validate(session):
        coupon = session.query(Coupon).filter(Coupon.code == code, Coupon.active == True).first()
        if not coupon:
            return False, None, "❌ کد تخفیف معتبر نیست."
        
        if coupon.expiry_date and coupon.expiry_date.replace(tzinfo=timezone.utc) < datetime.now(timezone.utc):
            return False, None, "❌ این کد تخفیف منقضی شده است."
        
        total_used = session.query(CouponUsage).filter(CouponUsage.coupon_id == coupon.id).count()
        if coupon.max_uses_total > 0 and total_used >= coupon.max_uses_total:
            return False, None, "❌ این کد تخفیف دیگر قابل استفاده نیست (تعداد استفاده‌ها به پایان رسیده)."
        
        user_used = session.query(CouponUsage).filter(
            CouponUsage.coupon_id == coupon.id,
            CouponUsage.user_id == user_id
        ).count()
        if coupon.max_uses_per_user > 0 and user_used >= coupon.max_uses_per_user:
            return False, None, "❌ شما قبلاً از این کد تخفیف استفاده کرده‌اید."
        
        # Check applicable_to
        applicable = coupon.applicable_to
        # Map action_type to applicable keys: 'new', 'renewal', 'topup'
        action_key = action_type.lower()
        if action_key == 'renew':
            action_key = 'renewal'
        if applicable != 'all':
            allowed = applicable.split(',')
            if action_key not in allowed:
                return False, None, "❌ این کد تخفیف برای این نوع خرید قابل استفاده نیست."
        
        return True, coupon, None
    
    if db_session is not None:
        valid, coupon, msg = _validate(db_session)
        return valid, coupon, msg
    else:
        with SessionLocal() as db:
            valid, coupon, msg = _validate(db)
            return valid, coupon, msg


def calculate_discount(original_price: int, action_type: str, coupon: Optional[Coupon] = None) -> Tuple[int, int, str]:
    """
    Calculate final price, discount amount, and description.
    
    Args:
        original_price: Original price in Toman
        action_type: 'NEW', 'RENEW', 'TOPUP', etc.
        coupon: Coupon object if applied, else None
    
    Returns:
        (final_price, discount_amount, discount_description)
    """
    if coupon:
        if coupon.discount_type == 'percent':
            discount = int(original_price * coupon.discount_value / 100)
        else:
            discount = min(coupon.discount_value, original_price)
        final_price = original_price - discount
        discount_desc = f"کد تخفیف {coupon.code}"
        return final_price, discount, discount_desc
    else:
        # Automatic discount for renewals and topups
        if action_type in ('RENEW', 'TOPUP'):
            with SessionLocal() as db:
                setting = db.query(AppSetting).filter(AppSetting.key == 'discount_percent').first()
                discount_pct = int(setting.value) if setting else 5
            discount = int(original_price * discount_pct / 100)
            final_price = original_price - discount
            discount_desc = f"تخفیف خودکار {discount_pct}%"
            return final_price, discount, discount_desc
        else:
            return original_price, 0, "بدون تخفیف"