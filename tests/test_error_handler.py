"""Locks the aiogram error-handler contract: on aiogram 3.x the @dp.errors()
observer passes ONE ``ErrorEvent`` object — declaring named parameters like
``(event, exception)`` crashes inside the handler itself and silently kills
alert delivery. Caught live right before deploy; this test keeps it fixed."""
import asyncio
import datetime as _dt

from aiogram import Dispatcher, Bot, F
from aiogram.types import Update, Message, Chat, User
from aiogram.client.default import DefaultBotProperties

from tests._bootstrap import bot, FakeState  # noqa: F401


def test_global_error_handler_runs_and_extracts_context():
    dp = Dispatcher()
    dp.errors.register(bot.global_error_handler)

    alerts = []

    async def fake_send_message(chat_id, text=None, *a, **k):
        alerts.append(text)
        return None

    @dp.message(F.text == "/boom")
    async def boom(message):
        raise KeyError('client_name')   # the exact historical crash

    async def main():
        b = Bot(token="123456:x", default=DefaultBotProperties(parse_mode="HTML"))
        # the handler DMs via the MODULE-LEVEL bot instance — intercept that
        b.send_message = fake_send_message
        bot.bot.send_message = fake_send_message
        upd = Update(update_id=1, message=Message(
            message_id=1,
            date=_dt.datetime.now(),
            chat=Chat(id=111, type="private"),
            from_user=User(id=111, is_bot=False, first_name="T"),
            text="/boom"))
        await dp.feed_update(b, upd)
        await b.session.close()

    asyncio.run(main())
    assert alerts, 'error handler never produced an alert!'
    assert 'KeyError' in alerts[0] and 'client_name' in alerts[0]
    assert 'پردازش آپدیت تلگرام' in alerts[0]


def test_noise_exception_never_reaches_alert_text():
    from src.utils.alerting import is_noise

    class FakeExc(Exception):
        pass

    # routine Telegram chatter must be filtered before any DM is composed
    assert is_noise(FakeExc("Telegram says: [400] message is not modified"))
    assert not is_noise(KeyError('client_name'))
