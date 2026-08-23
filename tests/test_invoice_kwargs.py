"""Regression tests for _build_invoice_kwargs — the exact seam that crashed
production with ``KeyError: 'client_name'`` when an Amnezia purchase reached
the receipt step stamped as a plain NEW."""
from tests._bootstrap import bot  # noqa: F401  (bootstrap must run first)


def test_amnezia_new_has_no_client_name_requirement():
    # The production-crash state: Amnezia flow data WITHOUT client_name.
    data = {'action_type': 'AMNEZIA_NEW', 'plan_id': 7,
            'amnezia_server_id': 1, 'amz_username': None, 'amz_password': None}
    kw = bot._build_invoice_kwargs(data)
    assert kw['action_type'] == 'AMNEZIA_NEW'
    assert kw['client_name'] is None
    assert kw['amnezia_service_id'] is None          # NEW has no service yet
    import json
    assert json.loads(kw['description']) == {'server_id': 1}


def test_amnezia_creds_ride_in_description():
    data = {'action_type': 'AMNEZIA_NEW', 'plan_id': 7,
            'amnezia_server_id': 0, 'amz_username': 'ali',
            'amz_password': 'Secret1'}
    kw = bot._build_invoice_kwargs(data)
    import json
    assert json.loads(kw['description']) == {
        'server_id': 0, 'amz_username': 'ali', 'amz_password': 'Secret1'}


def test_amnezia_renew_links_service_and_skips_description():
    data = {'action_type': 'AMNEZIA_RENEW', 'plan_id': 7,
            'target_service_id': 55, 'client_name': 'ignored'}
    kw = bot._build_invoice_kwargs(data)
    assert kw['amnezia_service_id'] == 55
    assert kw['description'] is None
    assert kw['client_name'] is None


def test_xui_new_keeps_entered_client_name():
    data = {'action_type': 'NEW', 'plan_id': 3, 'client_name': 'AliOffice'}
    kw = bot._build_invoice_kwargs(data)
    assert kw['client_name'] == 'AliOffice'
    assert kw['action_type'] == 'NEW'
    assert kw['amnezia_service_id'] is None and kw['description'] is None


def test_xui_renew_prefers_target_email():
    data = {'action_type': 'RENEW', 'target_email': 'ali_12', 'client_name': 'bare'}
    assert bot._build_invoice_kwargs(data)['client_name'] == 'ali_12'


def test_missing_action_defaults_to_new():
    kw = bot._build_invoice_kwargs({'client_name': 'x'})
    assert kw['action_type'] == 'NEW'
    assert kw['client_name'] == 'x'


def test_pricing_fields_flow_through():
    data = {'action_type': 'TOPUP', 'final_price': 95000, 'original_price': 100000,
            'discount_amount': 5000, 'coupon_code': 'C10'}
    kw = bot._build_invoice_kwargs(data)
    assert (kw['total_price'], kw['original_price'],
            kw['discount_amount'], kw['coupon_code']) == (95000, 100000, 5000, 'C10')
