"""Executes process_receipt END-TO-END with fakes — not just the pure kwargs
function. The handler family regressed twice (KeyError then NameError on
stale locals after refactors); these tests run the real coroutine so any
future refactor of its body fails loudly here instead of in production."""
import json

from tests._bootstrap import FakeMsg, FakeState, bot, seed_plan


class _FakePhoto:
    """Stands in for message.photo[-1]; .file_size and .file_id are read."""

    file_size = 100_000  # well under max
    file_id = 'test-photo-file-id'


class _FromUser:
    id = 555000
    full_name = 'Test Buyer'
    username = None


class ReceiptMessage(FakeMsg):
    """Message stand-in with chat/photo/from_user for the receipt handler."""

    def __init__(self, user_id=555000):
        super().__init__()
        self.chat = type('C', (), {'id': user_id})()
        self.from_user = type('U', (), {'id': user_id, 'full_name': 'Buyer',
                                        'username': None})()
        self.photo = [_FakePhoto()]


def _run_receipt(state_data, tg_id=555000):
    """Patch the global bot's Telegram I/O, drive process_receipt synchronously,
    return (persisted invoice dict, buyer-visible messages, admin captions)."""
    import asyncio
    from database import SessionLocal, Invoice

    sent_photos = []

    async def noop(*a, **k):
        return None

    async def fake_send_photo(admin_id, file_id, *a, **k):
        sent_photos.append(k.get('caption', ''))

    orig_download, orig_sca, orig_sp = (bot.bot.download, bot.bot.send_chat_action,
                                        bot.bot.send_photo)
    bot.bot.download = noop
    bot.bot.send_chat_action = noop
    bot.bot.send_photo = fake_send_photo

    msg = ReceiptMessage(tg_id)
    st = FakeState(state_data)
    try:
        asyncio.run(bot.process_receipt(msg, st))
    finally:
        bot.bot.download, bot.bot.send_chat_action, bot.bot.send_photo = (
            orig_download, orig_sca, orig_sp)

    with SessionLocal() as db:
        inv = db.query(Invoice).filter(
            Invoice.telegram_user_id == tg_id).order_by(
            Invoice.id.desc()).first()
        row = {c.name: getattr(inv, c.name) for c in inv.__table__.columns}
    return row, msg.sent, sent_photos


def test_xui_new_receipt_full_path():
    seed_plan(3, name='XUI 5GB', gb=5, days=30, price=50000, service_type='xui')
    data = {'action_type': 'NEW', 'plan_id': 3, 'client_name': 'AliOffice',
            'final_price': 50000, 'original_price': 50000,
            'discount_amount': 0, 'coupon_code': None}
    row, buyer_msgs, photos = _run_receipt(data)
    assert row['action_type'] == 'NEW'
    assert row['client_name'] == 'AliOffice'
    assert row['status'] == 'PENDING'
    assert len(photos) >= 1 and f"#{row['id']}" in photos[0]
    assert any('رسید شما با موفقیت ثبت شد' in (m or '') for m in buyer_msgs)


def test_amnezia_new_receipt_no_client_name_regression():
    """THE production NameError case: Amnezia data without client_name must
    persist cleanly and notify admins."""
    seed_plan(7)
    data = {'action_type': 'AMNEZIA_NEW', 'plan_id': 7,
            'amnezia_server_id': 1, 'amnezia_server_name': 'x',
            'amz_username': None, 'amz_password': None,
            'final_price': 100000, 'original_price': 100000,
            'discount_amount': 0, 'coupon_code': None}
    row, buyer_msgs, _ = _run_receipt(data, tg_id=555001)
    assert row['action_type'] == 'AMNEZIA_NEW'
    assert row['client_name'] is None
    assert json.loads(row['description']) == {'server_id': 1}
    assert any('رسید شما با موفقیت ثبت شد' in (m or '') for m in buyer_msgs)


def test_amnezia_topup_receipt_keeps_added_gb():
    data = {'action_type': 'AMNEZIA_TOPUP', 'target_service_id': 12,
            'added_gb': 5, 'final_price': 25000, 'original_price': 25000,
            'discount_amount': 0, 'coupon_code': None}
    row, _, _ = _run_receipt(data, tg_id=555002)
    assert row['action_type'] == 'AMNEZIA_TOPUP'
    assert row['added_gb'] == 5
    assert row['amnezia_service_id'] == 12
