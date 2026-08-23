"""Coupon applicability + admin-menu structure tests."""
from tests._bootstrap import bot  # noqa: F401


def test_coupon_action_key_mapping():
    assert bot._coupon_action_key('NEW') == 'new'
    assert bot._coupon_action_key('RENEW') == 'renewal'
    assert bot._coupon_action_key('TOPUP') == 'topup'
    assert bot._coupon_action_key('AMNEZIA_NEW') == 'new'
    assert bot._coupon_action_key('AMNEZIA_RENEW') == 'renewal'
    assert bot._coupon_action_key('AMNEZIA_TOPUP') == 'topup'
    assert bot._coupon_action_key(None) == 'new'


def test_admin_menu_preserves_every_action():
    from src.utils.keyboard import get_admin_menu, get_admin_category_kb, ADMIN_CATEGORIES
    top = [b.callback_data for row in get_admin_menu().inline_keyboard for b in row]
    # top level must be categories only — no action buttons leaked up
    for cat in ADMIN_CATEGORIES:
        assert cat in top
    assert 'admin_dashboard' not in top

    all_leaf = set()
    for cat, (title, items) in ADMIN_CATEGORIES.items():
        _, sub = get_admin_category_kb(cat)
        sub_cbs = [b.callback_data for row in sub.inline_keyboard for b in row]
        assert sub_cbs[-1] == 'admin_panel', f'{cat} missing back button'
        assert set(sub_cbs[:-1]) == {cb for _, cb in items}, f'{cat} buttons drifted'
        all_leaf |= set(sub_cbs[:-1])

    must_have = {'admin_dashboard', 'admin_select_inbound', 'admin_sys_status',
                 'admin_panel_traffic', 'admin_force_traffic', 'admin_add_plan',
                 'admin_add_plan_amz', 'admin_view_plans', 'admin_traffic_packs',
                 'admin_coupon_menu', 'admin_trial_settings', 'admin_referral_settings',
                 'admin_retry_invoices', 'admin_approved_receipts', 'admin_custom_receipt',
                 'admin_billing_report', 'admin_set_card', 'admin_manage_user',
                 'admin_del_depleted', 'admin_reseller_menu', 'admin_broadcast',
                 'admin_amnezia', 'admin_reconcile_names', 'admin_reconcile_user',
                 'admin_sync_group', 'admin_backup_tg', 'admin_restart_menu',
                 'admin_set_sub_link', 'admin_set_support'}
    assert not (must_have - all_leaf), f'lost actions: {must_have - all_leaf}'


def test_admin_category_unknown_id_falls_back():
    from src.utils.keyboard import get_admin_category_kb
    _, kb = get_admin_category_kb('admcat_nope')
    cbs = [b.callback_data for row in kb.inline_keyboard for b in row]
    assert 'admin_dashboard' in cbs and cbs[-1] == 'admin_panel'


def test_effect_constants_are_wellformed():
    import tasks
    for name in ('MSG_EFFECT_CONFETTI', 'MSG_EFFECT_FIRE'):
        val = getattr(tasks, name)
        assert isinstance(val, str) and val.isdigit() and len(val) >= 18, name
