"""One-off smoke test for AmneziaClient against the live panel. Safe to delete."""
import asyncio
import os
import sys
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

os.environ['SKIP_DB_INIT'] = 'true'
os.environ['DATABASE_URL'] = 'sqlite:///storage/test_amnezia.db'
os.environ['AMNEZIA_API_URL'] = 'https://rahanet.aethera.ir:5000'
os.environ.setdefault('AMNEZIA_API_USERNAME', os.getenv('AMNEZIA_API_USERNAME', ''))
os.environ.setdefault('AMNEZIA_API_PASSWORD', os.getenv('AMNEZIA_API_PASSWORD', ''))

import logging
logging.basicConfig(level=logging.INFO)

from database import Base, engine
Base.metadata.create_all(bind=engine)

from src.services.amnezia import AmneziaClient


async def main():
    c = AmneziaClient()
    try:
        servers = await c.list_servers()
        print('SERVERS:', servers)

        u = await c.ensure_panel_user(999999999)
        print('PANEL USER:', u.panel_user_id, u.username)

        # second call must hit the local mapping (no extra API calls)
        u2 = await c.ensure_panel_user(999999999)
        assert u2.panel_user_id == u.panel_user_id

        svc = await c.add_connection(u.panel_user_id, servers[0]['id'], f"smoke_{int(datetime.now().timestamp())}")
        print('CONNECTION:', svc['connection_id'], svc['client_id'][:20], 'server:', svc['server_name'])

        exp = datetime.now(timezone.utc) + timedelta(days=7)
        await c.update_user_limits(u.panel_user_id, 999999999, quota_gb=10, expiry=exp)
        stats = await c.get_user_stats(u.username)
        print('STATS: used=%(used)s limit=%(limit)s exp=%(expiration_date)s enabled=%(enabled)s' % {
            'used': stats['used'], 'limit': stats['limit'],
            'expiration_date': stats['expiration_date'], 'enabled': stats['enabled']})
        assert abs(stats['limit'] - 10 * 1024 ** 3) < 1024 ** 3, f"limit mismatch: {stats['limit']}"

        cfg = await c.get_connection_config(svc['server_id'], svc['client_id'])
        print('CONFIG keys:', sorted(cfg.keys()), '| vpn_link prefix:', (cfg['vpn_link'] or '')[:12])

        await c.remove_connection(svc['server_id'], svc['client_id'])
        await c.delete_panel_user(u.panel_user_id)
        print('CLEANUP OK — ALL ASSERTIONS PASSED')
    finally:
        await c.close()


asyncio.run(main())
