"""VLESS inbound-coverage audit tests: the pure membership parser plus
menu/confirmation wiring. Panel I/O is not exercised here (the audit task's
write path was verified live against the real panel)."""
from tests._bootstrap import bot, tasks  # noqa: F401


def _ib(id_, protocol='vless', enable=True, settings=None):
    ib = {"id": id_, "protocol": protocol, "enable": enable}
    if settings is not None:
        ib["settings"] = settings
    return ib


def _cl(email, enable=True):
    return {"email": email, "enable": enable, "totalGB": 100,
            "expiryTime": 123, "tgId": 5, "subId": "sub" + email}


def test_parses_dict_settings_and_computes_missing():
    inb = [
        _ib(1, settings={"clients": [_cl("a"), _cl("b")]}),
        _ib(2, settings={"clients": [_cl("a")]}),
    ]
    target, clients = tasks._parse_vless_membership(inb)
    assert target == [1, 2]
    assert clients["a"]["have"] == {1, 2} and clients["a"]["missing"] == set()
    assert clients["b"]["have"] == {1} and clients["b"]["missing"] == {2}


def test_settings_as_json_string_is_parsed():
    import json
    inb = [
        _ib(1, settings=json.dumps({"clients": [_cl("a")]})),
        _ib(2, settings=json.dumps({"clients": [_cl("a"), _cl("c")]})),
    ]
    target, clients = tasks._parse_vless_membership(inb)
    assert target == [1, 2]
    assert clients["a"]["missing"] == set()
    assert clients["c"]["missing"] == {1}
    assert clients["c"]["entry"]["subId"] == "subc"


def test_protocol_and_enable_filters():
    inb = [
        _ib(1),                                   # vless enabled -> target
        _ib(2, protocol="vmess"),                 # wrong protocol -> ignored
        _ib(3, enable=False),                     # disabled -> ignored
        _ib(4, settings={"clients": [_cl("a"), _cl("off", enable=False)]}),
    ]
    target, clients = tasks._parse_vless_membership(inb)
    assert target == [1, 4]                       # 4 is also an enabled vless inbound
    assert set(clients) == {"a"}                  # disabled client excluded
    # "a" was only listed under inbound 4 -> missing inbound 1
    assert clients["a"]["have"] == {4}
    assert clients["a"]["missing"] == {1}


def test_invalid_json_settings_treated_as_empty():
    inb = [_ib(1, settings="{not json"), _ib(2)]
    target, clients = tasks._parse_vless_membership(inb)
    assert target == [1, 2]
    assert clients == {}


def test_no_clients_edge():
    target, clients = tasks._parse_vless_membership([])
    assert target == [] and clients == {}


def test_menu_wiring_for_audit_button():
    from src.utils.keyboard import get_admin_category_kb
    _, kb = get_admin_category_kb('admcat_maint')
    cbs = [b.callback_data for row in kb.inline_keyboard for b in row]
    assert 'admin_vless_audit' in cbs

    src = open('bot.py', encoding='utf-8').read()
    assert 'admin_vless_audit_go' in src
    assert 'tasks.audit_vless_inbounds.delay' in src
