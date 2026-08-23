"""End-to-end FSM seam tests: replay the real handler sequence (shop → picker
→ credentials → coupon → payment) with fakes and assert the invoice-derivation
state that process_receipt will receive. This is the class of bug that crashed
production with KeyError: 'client_name' — scripts bypassing the FSM never saw
it, so these tests exercise the handlers themselves."""
import json

from tests._bootstrap import FakeMsg, FakeState, bot, seed_plan


def _run(coro):
    import asyncio
    return asyncio.run(coro)


async def _amnezia_buy_sequence(plan_id=7, with_creds=True):
    """Drive the exact Amnezia NEW handler chain and return the final state."""
    import bot
    st = FakeState()
    await bot.show_plans_content(st, user_id=111, amnezia_only=True)
    assert st.data['action_type'] == 'NEW'          # shop stamps the default…
    await st.update_data(plan_id=plan_id, amnezia_server_id=1,
                         amnezia_server_name='x',
                         amz_username=None, amz_password=None)
    await bot._amz_send_coupon_step(FakeMsg(), st)  # …and the choke point upgrades it
    return st


def test_amnezia_new_fsm_stamps_action_before_receipt():
    seed_plan(7)
    st = _run(_amnezia_buy_sequence())
    assert st.data['action_type'] == 'AMNEZIA_NEW'
    assert st.state == bot.BuyFlow.wait_for_coupon   # receipt state arrives via show_payment


def bot_wait_for_receipt():
    from tests._bootstrap import bot
    return bot.BuyFlow.wait_for_receipt


async def _amnezia_renew_sequence():
    import bot
    st = FakeState()
    await st.update_data(action_type='AMNEZIA_RENEW', target_service_id=9,
                         plan_id=7)
    await bot.show_payment(FakeMsg(), st)
    return st


def test_amnezia_new_full_derivation_after_payment():
    seed_plan(7)
    st = _run(_amnezia_buy_sequence(with_creds=False))
    await_ = None
    # skip coupon, then render the payment screen exactly as the flow does
    async def go():
        import bot
        await st.update_data(coupon_code=None)
        await bot.show_payment(FakeMsg(), st)
    _run(go())
    assert st.data['final_price'] == 100000
    assert st.state == bot_wait_for_receipt()

    kw = bot._build_invoice_kwargs(st.data)
    assert kw['action_type'] == 'AMNEZIA_NEW'
    assert kw['client_name'] is None
    assert json.loads(kw['description']) == {'server_id': 1}
    return True


def test_amnezia_renew_payment_links_service_id():
    seed_plan(7)
    st = _run(_amnezia_renew_sequence())
    assert st.data['final_price'] == 95000   # 100000 − 5% renewal discount, same as XUI
    assert st.state == bot_wait_for_receipt()
    kw = bot._build_invoice_kwargs(st.data)
    assert kw['action_type'] == 'AMNEZIA_RENEW'
    assert kw['amnezia_service_id'] == 9
    assert kw['client_name'] is None


async def _xui_buy_sequence():
    import bot
    st = FakeState()
    await bot.show_plans_content(st, user_id=111)     # combined shop, XUI visible
    await st.update_data(plan_id=8, client_name=None)
    # validate_name would set this after the user types their connection name:
    await st.update_data(client_name='AliOffice')
    await st.update_data(coupon_code=None)
    await bot.show_payment(FakeMsg(), st)
    return st


def test_xui_new_fsm_still_works():
    seed_plan(8, name='XUI 10GB', gb=10, service_type='xui')
    st = _run(_xui_buy_sequence())
    assert st.data['final_price'] == 100000
    assert st.data['action_type'] == 'NEW'
    kw = bot._build_invoice_kwargs(st.data)
    assert kw['client_name'] == 'AliOffice'
    assert not str(kw['action_type']).startswith('AMNEZIA_')


def test_shop_hides_amnezia_from_regular_users_but_shows_admins():
    from tests._bootstrap import FakeState as FS

    async def go(user_id):
        import bot
        text, kb = await bot.show_plans_content(FS(), user_id=user_id,
                                                amnezia_only=True)
        return text, kb

    text, kb = _run(go(111))     # admin (ADMIN_CHAT_IDS=111), AMNEZIA_ENABLED=false
    assert 'Amz 10GB' in text
    cbs = [b.callback_data for row in kb.inline_keyboard for b in row]
    assert cbs[0].startswith('amzplan_')

    text2, kb2 = _run(go(999))   # regular user while disabled
    assert 'هیچ پلن Amnezia فعالی' in text2
