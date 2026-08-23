"""Double-tap tolerance: re-running a screen draw whose content is already
on screen raises TelegramBadRequest('message is not modified') — handlers
must swallow it instead of crashing (logged as noise, never paged)."""
import asyncio

from aiogram.exceptions import TelegramBadRequest

from tests._bootstrap import bot, FakeState, FakeMsg, seed_plan


class _RigidMessage(FakeMsg):
    """edit_text that behaves like Telegram for identical redraws."""

    def __init__(self):
        super().__init__()
        self._last = None

    async def edit_text(self, text=None, *a, **k):
        if self._last == (text, str(k.get('reply_markup'))):
            raise TelegramBadRequest(method=None,
                                     message="Bad Request: message is not modified: "
                                             "specified new message content and reply markup "
                                             "are exactly the same as a current content")
        self._last = (text, str(k.get('reply_markup')))
        return await super().edit_text(text, *a, **k)


def test_prompt_custom_gb_survives_double_tap():
    seed_plan(8, name='XUI 10GB', gb=10, days=30, price=100000, service_type='xui')
    msg = _RigidMessage()
    st = FakeState({'action_type': 'TOPUP', 'plan_id': 8, 'plan_name': 'XUI 10GB',
                    'price_per_gb': 10000, 'discount_pct': 5})

    # first draw renders; an immediate second tap redraws identical content
    asyncio.run(bot.prompt_custom_gb(msg, '10000', st))
    asyncio.run(bot.prompt_custom_gb(msg, '10000', st))

    assert st.state == bot.TopupFlow.wait_for_gb
