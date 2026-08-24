"""Auto-failover tests: when an Amnezia server disappears and a new one
appears, stranded services are re-created on the healthy target with fresh
configs DM'd; without a target, the legacy warn-once path applies.
All panel I/O faked at tasks.AmneziaClient."""
import asyncio
from datetime import datetime, timedelta, timezone

from tests._bootstrap import bot as _b  # bootstrap first
import tasks
from database import SessionLocal, AmneziaService


def _clean_services():
    from database import engine
    from sqlalchemy import text
    from tests.test_amnezia_trial import _swap_services_table  # reuse DDL
    _swap_services_table()
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM amnezia_trials"))


def _seed(tg_id, server_id, server_name, is_trial=False):
    now = datetime.now(timezone.utc)
    with SessionLocal() as db:
        svc = AmneziaService(
            telegram_user_id=tg_id,
            connection_id=f'conn-{tg_id}',
            client_id=f'client-{tg_id}',
            server_id=server_id,
            server_name=server_name,
            protocol='awg2',
            name=f'svc_{tg_id}',
            quota_bytes=1024 ** 3,
            expiry_date=now + timedelta(days=10),
            status='active',
            is_trial=is_trial,
            panel_user_id=f'panel-{tg_id}',
            panel_username=f'u{tg_id}',
        )
        db.add(svc)
        db.commit()
        return svc.id


class FakeClient:
    """Scriptable: live server list + per-account connections."""

    def __init__(self, servers, connections_by_account):
        self._servers = servers
        self._conns = connections_by_account   # {panel_uid: [conn dicts]}
        self.added = []                        # (panel_uid, server_id, name)
        self.removed = []                      # (server_id, client_id)

    async def list_servers(self, force_refresh=False):
        return self._servers

    async def get_user_connections(self, panel_user_id):
        return self._conns.get(panel_user_id, [])

    async def add_connection(self, panel_user_id, server_id, name):
        self.added.append((panel_user_id, server_id, name))
        return {"connection_id": f"new-{len(self.added)}", "client_id": f"newc-{len(self.added)}",
                "server_id": server_id, "protocol": "awg2", "server_name": f"s{server_id}",
                "config": "[Interface]", "vpn_link": "vpn://NEW"}

    async def get_connection_config(self, server_id, client_id):
        return {"config": "[Interface]", "vpn_link": "vpn://NEW"}

    async def remove_connection(self, server_id, client_id):
        self.removed.append((server_id, client_id))

    async def close(self):
        pass


def _patch(client, sent):
    """Patch panel client + all notification sinks. Callers MUST restore
    ALL four originals in finally — leaking notify patches poisons later
    tests (awaiting a sync fake raises TypeError mid-sweep)."""
    orig = [tasks.AmneziaClient, tasks.notify_many_users,
            tasks.notify_user_with_buttons, tasks.send_user_document]
    tasks.AmneziaClient = lambda *a, **k: client

    async def noop(*a, **k):
        return None

    tasks.notify_many_users = noop
    tasks.notify_user_with_buttons = noop
    tasks.send_user_document = noop
    return orig, noop


def test_stranded_service_migrates_to_newly_added_server():
    _clean_services()
    sid = _seed(888001, server_id=5, server_name='dead-loc')
    # old server gone entirely; a NEW server (id 2) was just added
    client = FakeClient(servers=[{'id': 2, 'name': 'fresh-loc', 'alive': True}],
                        connections_by_account={'panel-888001': []})
    orig, noop = _patch(client, [])
    try:
        tasks.check_amnezia_expiry.run()  # unrelated sweep must not crash
        tasks.check_amnezia_servers.run()
    finally:
        tasks.AmneziaClient, tasks.notify_many_users = orig[0], orig[1]
        tasks.notify_user_with_buttons, tasks.send_user_document = orig[2], orig[3]

    with SessionLocal() as db:
        svc = db.query(AmneziaService).filter(AmneziaService.id == sid).first()
        assert svc.server_id == 2 and svc.server_name == 's2'
        assert svc.connection_id.startswith('new-')
        assert svc.status == 'active' and svc.server_missing_notified_at is None
    assert len(client.added) == 1 and client.added[0][1] == 2
    assert client.removed == []      # nothing to remove — old server is gone


def test_unreachable_server_migrates_and_removes_old_connection():
    _clean_services()
    sid = _seed(888002, server_id=0, server_name='flaky')
    client = FakeClient(servers=[{'id': 0, 'name': 'flaky', 'alive': False},
                                 {'id': 1, 'name': 'healthy', 'alive': True}],
                        connections_by_account={
                            'panel-888002': [{'id': 'conn-888002',
                                              'client_id': 'client-888002',
                                              'server_id': 0,
                                              'server_name': 'flaky'}]})
    orig_list, _noop = _patch(client, [])
    try:
        tasks.check_amnezia_servers.run()
    finally:
        tasks.AmneziaClient, tasks.notify_many_users = orig_list[0], orig_list[1]
        tasks.notify_user_with_buttons, tasks.send_user_document = orig_list[2], orig_list[3]

    with SessionLocal() as db:
        svc = db.query(AmneziaService).filter(AmneziaService.id == sid).first()
        assert svc.server_id == 1
    assert svc.server_name == 's1'   # panel record's own name wins
    assert client.added and client.added[0][1] == 1
    assert client.removed == [(0, 'client-888002')]


def test_no_alternative_keeps_warn_once():
    _clean_services()
    sid = _seed(888003, server_id=3, server_name='only-one')
    client = FakeClient(servers=[{'id': 3, 'name': 'only-one', 'alive': False}],
                        connections_by_account={})
    client._conns['panel-888003'] = [{'id': 'conn-888003',
                                      'client_id': 'client-888003',
                                      'server_id': 3,
                                      'server_name': 'only-one'}]
    orig, _noop = _patch(client, [])
    sent = []

    async def fake_notify(ns):
        sent.extend(ns)
    tasks.notify_many_users = fake_notify
    tasks.notify_user_with_buttons = fake_notify
    tasks.send_user_document = fake_notify
    try:
        tasks.check_amnezia_servers.run()
    finally:
        tasks.AmneziaClient, tasks.notify_many_users = orig[0], orig[1]
        tasks.notify_user_with_buttons, tasks.send_user_document = orig[2], orig[3]

    with SessionLocal() as db:
        svc = db.query(AmneziaService).filter(AmneziaService.id == sid).first()
        assert svc.status == 'active'          # untouched — nowhere to migrate
        assert svc.server_missing_notified_at is not None
    assert any('در دسترس نیست' in (n[1] or '') for n in sent)
    assert client.added == []                  # no target -> no migration attempt


def test_pick_migration_target_prefers_added():
    servers = [{'id': 0, 'alive': False}, {'id': 1, 'alive': True}, {'id': 2, 'alive': True}]
    alive = {0: False, 1: True, 2: True}
    # newly added wins
    assert tasks._pick_migration_target(servers, alive, added_ids=[2], exclude_id=None) == 2
    # fallback: first alive excluding current
    assert tasks._pick_migration_target(servers, alive, added_ids=[], exclude_id=1) == 2
    # only one server and it's the excluded one -> None
    assert tasks._pick_migration_target([{'id': 0, 'alive': True}], {0: True}, [], exclude_id=0) is None
