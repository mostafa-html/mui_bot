"""Regression tests for the res_toggle_/res_delete_ callback collision and
the watchdog _admin_ids helper.

Collision history: the ADMIN reseller-account actions (toggle / delete /
delete-confirm) registered plain ``startswith("res_*")`` prefixes that were
also used by reseller SERVICE actions. aiogram continues propagation when a
handler returns None, so deactivating a config ALSO flipped the reseller's
own account inactive — locking them out of re-activating it. The admin side
now uses admres_ prefixes; these tests lock that contract."""
import re

from tests._bootstrap import bot, tasks  # noqa: F401


def test_admin_ids_parses_env():
    cases = [
        ('111', [111]),
        ('111 , 222', [111, 222]),
        ('111,,222,', [111, 222]),      # empties ignored
        ('', []),                        # no admins configured
    ]
    for raw, expected in cases:
        tasks.os.environ['ADMIN_CHAT_IDS'] = raw
        assert tasks._admin_ids() == expected, raw


def test_only_one_res_toggle_and_res_delete_service_registration():
    """Exactly ONE startswith registration may exist per colliding family —
    a second (admin) one is what caused the reseller lock-out."""
    src = open('bot.py', encoding='utf-8').read()
    assert len(re.findall(r'F\.data\.startswith\("res_toggle_"\)', src)) == 1
    assert len(re.findall(r'F\.data\.startswith\("res_delete_"\)', src)) == 1
    # admin side renamed:
    assert 'admres_toggle_' in src and 'admres_delete_confirm_' in src


def test_no_admres_collision_with_service_handlers():
    src = open('bot.py', encoding='utf-8').read()
    # service handlers must not have been caught by the rename sweep
    assert 'callback_data=f"res_toggle_{email}"' in src
    assert 'callback_data=f"res_delete_{email}"' in src
    # and nothing registers a plain-startswith on the new admin prefixes
    assert 'F.data.startswith("admres_")' not in src


def test_watchdog_tasks_reference_defined_helper():
    import inspect
    src = open('tasks.py', encoding='utf-8').read()
    assert 'def _admin_ids()' in src
    # every _admin_ids( call site now resolves against the definition above
    assert src.count('_admin_ids(') >= 4
    sig = inspect.signature(tasks._admin_ids)
    assert list(sig.parameters) == []
