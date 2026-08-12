import json
import logging
from datetime import datetime, timezone, timedelta

from database import SessionLocal, Reseller, ResellerPack, Invoice
from xui_client import XUIClient

GB = 1024 ** 3
logger = logging.getLogger(__name__)


def is_reseller(user_id: int) -> bool:
    """True if the user is an active reseller (one indexed PK lookup)."""
    with SessionLocal() as db:
        rec = db.query(Reseller).filter(
            Reseller.telegram_user_id == user_id,
            Reseller.is_active == True
        ).first()
        return rec is not None


def get_reseller_balance(user_id: int):
    """Return (total_available_bytes, total_used_bytes) for an active reseller, or None.
    Total is derived solely from ResellerPack entries (legacy fields are deprecated and ignored)."""
    with SessionLocal() as db:
        reseller = db.query(Reseller).filter(
            Reseller.telegram_user_id == user_id,
            Reseller.is_active == True
        ).first()
        if not reseller:
            logger.info(f"get_reseller_balance: reseller not found or inactive for {user_id}")
            return None
        # Only use packs (legacy fields are ignored after migration)
        now = datetime.now(timezone.utc)
        packs = db.query(ResellerPack).filter(
            ResellerPack.reseller_id == user_id,
            ResellerPack.is_active == True,
            ResellerPack.expiry_date > now
        ).all()
        total_available = sum(p.granted_bytes - p.used_bytes for p in packs)
        total_used = sum(p.used_bytes for p in packs)
        logger.info(f"get_reseller_balance (pack-only): user={user_id}, total_available={total_available}, total_used={total_used}, pack_count={len(packs)}")
        return total_available, total_used


def reserve_reseller_allowance(user_id: int, needed_bytes: int, db_session=None) -> tuple:
    """Atomically reserve allowance from active packs (FIFO by expiry).
    Returns (success, reservation_data) where reservation_data is a dict:
        {'packs': [{'id': pack_id, 'deducted': bytes}, ...]}
    If db_session is provided, the transaction is NOT committed; caller must commit.
    Otherwise, uses its own session and commits.
    Uses row locks to prevent race conditions."""
    logger.info(f"reserve_reseller_allowance (pack-only): user={user_id}, needed_bytes={needed_bytes}")
    def _do_reserve(session):
        reseller = session.query(Reseller).filter(
            Reseller.telegram_user_id == user_id,
            Reseller.is_active == True
        ).with_for_update().first()
        if not reseller:
            logger.info(f"reserve_reseller_allowance: reseller not found or inactive for user {user_id}")
            return False, None

        now = datetime.now(timezone.utc)
        packs = session.query(ResellerPack).filter(
            ResellerPack.reseller_id == user_id,
            ResellerPack.is_active == True,
            ResellerPack.expiry_date > now
        ).with_for_update().order_by(ResellerPack.expiry_date.asc()).all()
        pack_available = sum(p.granted_bytes - p.used_bytes for p in packs)

        logger.info(f"reserve_reseller_allowance: pack_available={pack_available}, needed={needed_bytes}, pack_count={len(packs)}")

        if pack_available < needed_bytes:
            logger.info(f"reserve_reseller_allowance: insufficient allowance (pack_available={pack_available} < needed={needed_bytes})")
            return False, None

        remaining = needed_bytes
        packs_data = []

        # Deduct from packs (earliest expiry first)
        for pack in packs:
            if remaining <= 0:
                break
            available = pack.granted_bytes - pack.used_bytes
            if available <= 0:
                continue
            deduct = min(available, remaining)
            pack.used_bytes += deduct
            packs_data.append({'id': pack.id, 'deducted': deduct})
            logger.info(f"reserve_reseller_allowance: deducted {deduct} from pack {pack.id} (granted={pack.granted_bytes}, used={pack.used_bytes})")
            remaining -= deduct
            if pack.used_bytes >= pack.granted_bytes:
                pack.is_active = False
                logger.info(f"reserve_reseller_allowance: pack {pack.id} fully used, deactivated")

        if remaining > 0:
            # Should not happen if pack_available check passed
            logger.error(f"reserve_reseller_allowance: remaining > 0 after deduction! remaining={remaining}")
            return False, None

        reservation_data = {'packs': packs_data}
        logger.info(f"reserve_reseller_allowance: success, reservation_data={reservation_data}")
        return True, reservation_data

    if db_session is not None:
        # Use provided session, do not commit
        success, data = _do_reserve(db_session)
        return success, data
    else:
        with SessionLocal() as db:
            success, data = _do_reserve(db)
            if success:
                db.commit()
            else:
                db.rollback()
            return success, data


def reseller_owns_email(user_id: int, email: str) -> bool:
    """True if this email belongs to a COMPLETE reseller invoice owned by user_id."""
    if not is_reseller(user_id):
        return False
    with SessionLocal() as db:
        inv = db.query(Invoice).filter(
            Invoice.reseller_id == user_id,
            Invoice.client_name == email,
            Invoice.status == "COMPLETE"
        ).first()
        return inv is not None


def user_owns_email(user_id: int, email: str) -> bool:
    """True if this email belongs to a COMPLETE invoice owned by user_id (regular user)."""
    with SessionLocal() as db:
        inv = db.query(Invoice).filter(
            Invoice.telegram_user_id == user_id,
            Invoice.client_name == email,
            Invoice.status == "COMPLETE"
        ).first()
        return inv is not None