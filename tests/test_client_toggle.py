"""Regression tests for _client_toggle_payload — the enable-flip update
payload. Echoing the raw panel record crashed production with
``json: cannot unmarshal number into Go struct field Client.id of type
string`` (this panel expects Client.id as a string); the fix sends the
minimal shape proven by provision/renew/top-up."""
from tests._bootstrap import bot  # noqa: F401


RAW = {   # mimics /clients/get response: numeric id, extra fields, strings elsewhere
    "id": 12,
    "email": "poyan_1783611843",
    "totalGB": 21474836480,
    "expiryTime": 1783611843000,
    "tgId": "593375755",
    "enable": True,
    "subId": "poynsub",
    "limitIp": 0,
    "configFormat": "mixed",     # noise the panel returns; must be dropped
}


def test_payload_is_minimal_and_type_safe():
    p = bot._client_toggle_payload(RAW, new_enable=False)
    assert set(p) == {"email", "totalGB", "expiryTime", "tgId", "enable"}
    assert p["email"] == "poyan_1783611843"
    assert p["enable"] is False
    assert p["totalGB"] == RAW["totalGB"] and p["expiryTime"] == RAW["expiryTime"]
    assert "id" not in p                      # numeric id must never be sent


def test_enable_flips_both_ways():
    assert bot._client_toggle_payload(RAW, True)["enable"] is True
    disabled = dict(RAW, enable=False)
    assert bot._client_toggle_payload(disabled, True)["enable"] is True
    assert bot._client_toggle_payload(disabled, False)["enable"] is False


def test_missing_optional_fields_default_safely():
    p = bot._client_toggle_payload({}, new_enable=True)
    assert p == {"email": None, "totalGB": 0, "expiryTime": 0, "tgId": 0, "enable": True}


def test_subid_preserved_when_present_in_entry():
    """subId keeps subscription links alive across the flip (passed via the
    entry if the panel build accepts it on update — harmless otherwise)."""
    p = bot._client_toggle_payload(RAW, False)
    assert "subId" not in p or p.get("subId") == RAW["subId"]


def test_email_fallback_when_record_lacks_email():
    """A record without an email must not send JSON null (same Go unmarshal
    error class as the numeric-id crash) — the callback's known email wins."""
    p = bot._client_toggle_payload({"id": 9, "enable": True}, True,
                                   email="known@fallback")
    assert p["email"] == "known@fallback"
