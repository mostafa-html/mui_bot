"""
Amnezia Web Panel API client.

All behaviour below was verified against the live panel (2026-08-22):

* Auth          : session login (``POST /api/auth/login``). API tokens
                  (``Authorization: Bearer awp_...``) are READ-ONLY on the
                  current panel version — every user-mutating POST returns 403
                  with a token, so the client keeps a login session instead
                  and re-logins transparently when it expires.
* Servers       : no list endpoint exists — discover by ping-sweeping
                  ``GET /api/servers/{id}/ping`` from 0 until
                  ``{"error": "Server not found"}``. Server *names* are only
                  exposed on connection records (``server_name``), so they are
                  remembered opportunistically.
* Provisioning  : create a panel user (``telegramId`` set), then add a
                  connection under it. The add-response already contains the
                  full ``config`` text and ``vpn://`` deep link.
* Quota/expiry  : live on the PANEL USER, not the connection.
                  ``POST /api/users/{id}/update`` expects ``traffic_limit`` in
                  whole GB and ``expiration_date`` as ``YYYY-MM-DDTHH:MM``;
                  read endpoints return ``traffic_*`` in BYTES.
* Usage stats   : ``GET /api/users?search=<username>`` gives traffic_used,
                  traffic_limit, expiration_date, enabled.

This module performs NO import-time side effects (same contract as
``src/services/reseller.py``): the caller sets up the environment.
"""
import asyncio
import logging
import os
import secrets
import time
from datetime import datetime, timedelta, timezone

import httpx

from database import SessionLocal, AmneziaUser

GB = 1024 ** 3
PANEL_PAGE_SCAN_LIMIT = 10   # pages of 50 users scanned when adopting by telegramId
SERVER_SWEEP_MAX = 64        # hard stop for the ping sweep
CACHE_TTL_SECONDS = 3600
SERVER_NAMES_KEY = "amnezia_server_names"   # Redis hash: server_id -> name

# Official client apps (store entries verified live 2026-08-22 via
# itunes.apple.com search API + Play Store HTTP check).
PLAY_STORE_URL = "https://play.google.com/store/apps/details?id=org.amnezia.vpn"
APP_STORE_URL = "https://apps.apple.com/app/amneziavpn/id1600529900"

logger = logging.getLogger(__name__)


class AmneziaError(Exception):
    """Raised when the panel returns an error or an unexpected response."""


class AmneziaAuthError(AmneziaError):
    """Raised when login fails (bad credentials or captcha required)."""


class AmneziaUsernameTaken(AmneziaError):
    """A panel user with the requested username already exists (different owner)."""

    def __init__(self, username: str):
        self.username = username
        super().__init__(f"panel username already taken: {username}")


def format_expiration(dt: datetime) -> str:
    """Panel format for expiration_date: YYYY-MM-DDTHH:MM (minute precision)."""
    return dt.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M')


def parse_expiration(raw: str) -> datetime | None:
    """Parse a panel expiration_date string into an aware UTC datetime."""
    if not raw:
        return None
    try:
        return datetime.strptime(raw, '%Y-%m-%dT%H:%M').replace(tzinfo=timezone.utc)
    except ValueError:
        try:
            return datetime.fromisoformat(raw).astimezone(timezone.utc)
        except ValueError:
            logger.warning(f"amnezia: unparseable expiration_date: {raw!r}")
            return None


class AmneziaClient:
    def __init__(self, api_url: str = None, username: str = None,
                 password: str = None, protocol: str = None):
        from src.config import (AMNEZIA_API_URL, AMNEZIA_API_USERNAME,
                                AMNEZIA_API_PASSWORD, AMNEZIA_PROTOCOL)
        self.api_url = (api_url or AMNEZIA_API_URL).rstrip('/')
        self.username = username or AMNEZIA_API_USERNAME
        self.password = password or AMNEZIA_API_PASSWORD
        self.protocol = protocol or AMNEZIA_PROTOCOL or 'awg2'
        self._client: httpx.AsyncClient | None = None
        self._logged_in = False
        self._redis = None
        # In-process cache: (monotonic_ts, [{"id": int, "name": str|None, "alive": bool}, ...])
        # Cheap to rebuild (a handful of pings); each process refreshes its own.
        self._servers_cache: tuple | None = None

    # ---------- low level ----------

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                base_url=self.api_url,
                timeout=60.0,
                verify=False,  # panel may use self-signed certs (mirrors PANEL_SSL_VERIFY=false default)
            )
        return self._client

    async def close(self):
        self._logged_in = False
        if self._client is not None and not self._client.is_closed:
            await self._client.aclose()

    async def _ensure_logged_in(self):
        if self._logged_in:
            return
        client = await self._get_client()
        try:
            resp = await client.post('/api/auth/login', json={
                "username": self.username,
                "password": self.password,
                "captcha": "",
            })
        except httpx.HTTPError as e:
            raise AmneziaError(f"login request failed: {e}") from e
        if resp.status_code == 401:
            raise AmneziaAuthError("login rejected: bad credentials")
        if resp.status_code >= 400:
            # A captcha-enabled panel answers here; enable captcha in the panel
            # settings OFF for the bot account, or extend this to solve it.
            raise AmneziaAuthError(
                f"login failed HTTP {resp.status_code} "
                f"(is captcha enabled on the panel?): {resp.text[:200]}")
        data = resp.json() if resp.headers.get('content-type', '').startswith('application/json') else {}
        if data.get('status') != 'success':
            raise AmneziaAuthError(f"login failed: {data}")
        self._logged_in = True
        logger.info(f"amnezia: logged in as {self.username}")

    async def _request(self, method: str, path: str, json_body: dict = None,
                       params: dict = None, timeout: float = None) -> dict:
        """Perform one API call.

        Retries once per failure mode: re-login on session expiry (401/403),
        and a fresh connection on transport errors (the panel closes idle
        keep-alive connections, which surfaces as a read error on reuse).
        ``timeout`` overrides the client default for slow calls.
        """
        req_kwargs = {"json": json_body, "params": params}
        if timeout is not None:
            req_kwargs["timeout"] = timeout
        client = await self._get_client()
        await self._ensure_logged_in()
        try:
            resp = await client.request(method, path, **req_kwargs)
            if resp.status_code in (401, 403):
                logger.info(f"amnezia: {resp.status_code} on {method} {path}; re-login and retry")
                self._logged_in = False
                await self._ensure_logged_in()
                resp = await client.request(method, path, **req_kwargs)
        except httpx.HTTPError as e:
            logger.info(f"amnezia: transport error on {method} {path} ({e!r}); retrying on a fresh connection")
            if self._client is not None and not self._client.is_closed:
                await self._client.aclose()
            self._client = None
            self._logged_in = False
            client = await self._get_client()
            try:
                await self._ensure_logged_in()
                resp = await client.request(method, path, **req_kwargs)
            except httpx.HTTPError as e2:
                raise AmneziaError(f"panel request failed twice: {method} {path}: {e2}") from e2
        if resp.status_code >= 400:
            raise AmneziaError(f"panel HTTP {resp.status_code} for {method} {path}: {resp.text[:200]}")
        try:
            return resp.json()
        except ValueError as e:
            raise AmneziaError(f"panel returned non-JSON for {method} {path}") from e

    @staticmethod
    def _check_success(data: dict, action: str):
        if not isinstance(data, dict) or data.get('status') != 'success':
            raise AmneziaError(f"{action} failed: {data}")

    # ---------- servers ----------

    async def list_servers(self, force_refresh: bool = False) -> list[dict]:
        """Discover all panel servers via ping sweep.

        Returns [{"id": <int>, "name": <str|None>, "alive": <bool>}, ...] ordered by id.
        A server that exists but is unreachable stays in the list (alive=False);
        only the definitive "Server not found" answer ends the sweep.
        Names are filled in when known from previously seen connections.
        """
        if not force_refresh and self._servers_cache:
            ts, servers = self._servers_cache
            if time.monotonic() - ts < CACHE_TTL_SECONDS:
                return servers

        await self._ensure_logged_in()
        servers = []
        client = await self._get_client()
        for sid in range(SERVER_SWEEP_MAX):
            try:
                resp = await client.get(f"/api/servers/{sid}/ping")
            except httpx.HTTPError as e:
                raise AmneziaError(f"ping sweep failed at server {sid}: {e}") from e
            if resp.status_code == 401:
                # Session expired mid-sweep — re-login once and restart.
                self._logged_in = False
                await self._ensure_logged_in()
                return await self.list_servers(force_refresh=True)
            if resp.status_code >= 400:
                break  # 404 => past the last server
            try:
                data = resp.json()
            except ValueError:
                break
            if not isinstance(data, dict):
                break
            err = data.get('error')
            if err:
                if 'not found' in str(err).lower():
                    break  # definitive end of the server list
                break
            # alive:false means the server EXISTS but is currently unreachable —
            # keep it in the list and continue sweeping past it.
            servers.append({"id": sid, "name": None, "alive": bool(data.get('alive', False))})
        self._servers_cache = (time.monotonic(), servers)
        logger.info(f"amnezia: discovered {len(servers)} server(s)")
        return servers

    def remember_server_name(self, server_id: int, name: str):
        """Attach a server name learned from a connection record to the caches.

        Fire-and-forget when called from sync context is not possible (Redis
        call is async) — use :meth:`remember_server_name_async` where possible;
        this variant only updates the in-process cache.
        """
        if not name or not self._servers_cache:
            return
        ts, servers = self._servers_cache
        for s in servers:
            if s["id"] == server_id and not s["name"]:
                s["name"] = name

    async def _get_redis(self):
        """Lazy Redis connection for the shared name cache; None if unavailable."""
        if self._redis is None:
            url = os.getenv('REDIS_URL')
            if not url:
                return None
            try:
                import redis.asyncio as aioredis
                self._redis = aioredis.from_url(url, decode_responses=True)
            except Exception:
                return None
        return self._redis

    async def remember_server_name_async(self, server_id: int, name: str):
        """Persist a discovered server name (in-process + Redis, best effort)."""
        if not name:
            return
        self.remember_server_name(server_id, name)
        r = await self._get_redis()
        if r is None:
            return
        try:
            await r.hset(SERVER_NAMES_KEY, str(server_id), name)
        except Exception as e:
            logger.debug(f"amnezia: name cache write skipped: {e}")

    async def _load_persisted_names(self) -> dict:
        r = await self._get_redis()
        if r is None:
            return {}
        try:
            raw = await r.hgetall(SERVER_NAMES_KEY)
            return {int(k): v for k, v in (raw or {}).items()}
        except Exception:
            return {}

    async def replace_server_names(self, mapping: dict):
        """Wholesale-replace the persisted name cache with live-observed pairs.

        Called by the hourly sweep with {server_id: name} taken straight from
        connection records, so the cache self-heals after any server
        reorder/rename instead of serving stale entries forever.
        """
        if not mapping:
            return
        r = await self._get_redis()
        if r is None:
            return
        try:
            await r.delete(SERVER_NAMES_KEY)
            await r.hset(SERVER_NAMES_KEY,
                         mapping={str(k): str(v) for k, v in mapping.items()})
        except Exception as e:
            logger.debug(f"amnezia: name cache rewrite skipped: {e}")

    async def resolve_server_names(self, server_ids: list) -> dict:
        """Best-effort {server_id: name} for the given ids.

        The panel exposes no server-list endpoint; names only surface on
        connection records. Resolution order:
          1. FRESH harvest from our own managed panel accounts — overrides the
             cache, so a recent reorder/rename shows correct names immediately;
          2. persisted Redis cache fills any remaining unknowns (and is healed
             hourly by the sweep's ``replace_server_names``);
          3. one-time fallback scan of the panel user list for never-seen ids.
        """
        names = await self._load_persisted_names()

        # 1) Fresh evidence from accounts we manage (bounded) — fresh wins.
        from database import AmneziaService
        with SessionLocal() as db:
            managed_ids = [r[0] for r in db.query(AmneziaService.panel_user_id).filter(
                AmneziaService.panel_user_id.isnot(None),
                AmneziaService.status != 'deleted').limit(10).all()]
        for pid in managed_ids:
            try:
                conns = await self.get_user_connections(pid)
            except Exception:
                continue
            for c in conns:
                sid, name = c.get('server_id'), c.get('server_name')
                if name:
                    names[sid] = name
                    await self.remember_server_name_async(sid, name)

        missing = [sid for sid in server_ids if sid not in names]
        if not missing:
            return names

        # 3) One-time discovery for ids no connection has ever been seen on.
        async def _harvest(panel_user_id):
            nonlocal missing
            try:
                conns = await self.get_user_connections(panel_user_id)
            except Exception:
                return
            for c in conns:
                sid, name = c.get('server_id'), c.get('server_name')
                if sid in missing and name:
                    names[sid] = name
                    missing.remove(sid)
                    await self.remember_server_name_async(sid, name)

        page = 1
        while missing and page <= 3:
            try:
                data = await self._request('GET', '/api/users',
                                           params={"page": page, "size": 50})
            except Exception:
                break
            users = data.get('users', [])
            for u in users:
                if not missing:
                    break
                await _harvest(u.get('id'))
            if page >= data.get('pages', 1):
                break
            page += 1
        return names

    async def list_servers_detailed(self) -> list[dict]:
        """list_servers() enriched with resolved names (for user-facing pickers)."""
        servers = await self.list_servers()
        try:
            names = await self.resolve_server_names([s['id'] for s in servers])
        except Exception as e:
            logger.warning(f"amnezia: server-name resolution failed: {e}")
            names = {}
        for s in servers:
            if not s.get('name'):
                s['name'] = names.get(s['id'])
        return servers

    # ---------- panel users ----------

    async def find_panel_user_by_telegram(self, telegram_id: int) -> dict | None:
        """Scan the panel user list for an account whose telegramId matches."""
        for page in range(1, PANEL_PAGE_SCAN_LIMIT + 1):
            data = await self._request('GET', '/api/users',
                                       params={"page": page, "size": 50})
            users = data.get('users', [])
            for u in users:
                if u.get('telegramId') and str(u['telegramId']) == str(telegram_id):
                    return u
            if page >= data.get('pages', 1):
                break
        return None

    async def ensure_panel_user(self, telegram_id: int, username: str = None,
                                password: str = None) -> AmneziaUser:
        """Return the AmneziaUser mapping for this Telegram user, creating the
        panel account (and the local mapping row) if needed.

        ``username``/``password`` are the OPTIONAL credentials the user chose at
        purchase time; they only apply when a new panel account is created.
        The password is stored on the mapping row so the bot can show it back.

        Adoption order: local mapping -> exact username match -> telegramId scan
        (accounts created manually in the panel UI) -> create.

        Raises :class:`AmneziaUsernameTaken` if ``username`` belongs to another
        panel account.
        """
        with SessionLocal() as db:
            row = db.query(AmneziaUser).filter(
                AmneziaUser.telegram_user_id == telegram_id).first()
            if row:
                return row

        desired_username = username or f"tg_{telegram_id}"
        data = await self._request('GET', '/api/users',
                                   params={"search": desired_username, "page": 1, "size": 10})
        panel_user = next(
            (u for u in data.get('users', []) if u.get('username') == desired_username), None)
        if panel_user and str(panel_user.get('telegramId') or '') != str(telegram_id):
            raise AmneziaUsernameTaken(desired_username)

        if panel_user is None:
            panel_user = await self.find_panel_user_by_telegram(telegram_id)

        created_password = None
        if panel_user is None:
            created_password = password or secrets.token_urlsafe(16)
            resp = await self._request('POST', '/api/users/add', json_body={
                "username": desired_username,
                "password": created_password,
                "role": "user",
                "telegramId": str(telegram_id),
                "email": None,
                "description": "created by VpnBot-m",
                "traffic_limit": 0,
                "traffic_reset_strategy": "never",
                "expiration_date": None,
            })
            self._check_success(resp, f"create panel user {desired_username}")
            uid = resp.get('user_id')
            if not uid:
                raise AmneziaError(f"user/add returned no user_id: {resp}")
            panel_user = {"id": uid, "username": desired_username}
            logger.info(f"amnezia: created panel user {desired_username} ({uid})")
        else:
            logger.info(f"amnezia: adopted panel user {panel_user.get('username')} "
                        f"({panel_user.get('id')}) for telegram {telegram_id}")

        with SessionLocal() as db:
            row = db.query(AmneziaUser).filter(
                AmneziaUser.telegram_user_id == telegram_id).first()
            if row is None:
                row = AmneziaUser(
                    telegram_user_id=telegram_id,
                    panel_user_id=panel_user['id'],
                    username=panel_user['username'],
                    panel_password=created_password,
                )
                db.add(row)
                db.commit()
                db.refresh(row)
            return row

    async def is_username_available(self, username: str, telegram_id: int) -> bool:
        """False if a panel user with this exact username exists and belongs to
        a different Telegram user."""
        data = await self._request('GET', '/api/users',
                                   params={"search": username, "page": 1, "size": 10})
        for u in data.get('users', []):
            if (u.get('username') == username and
                    str(u.get('telegramId') or '') != str(telegram_id)):
                return False
        return True

    async def ensure_service_account(self, telegram_id: int, base_username: str = None,
                                     password: str = None, invoice_id: int = None) -> dict:
        """Create a FRESH dedicated panel account for one subscription.

        Separate-accounts model: every purchase gets its own panel user so its
        quota/expiry/enable state never leaks into the buyer's other services.
        No adoption of pre-existing accounts happens here by design.

        Username: ``<base>_<invoice_id>`` (base = chosen username or tg_<id>);
        when ``invoice_id`` is None the bare base is used (trials); a random
        suffix is appended on the rare collision.

        Returns {"panel_user_id", "username", "password"}.
        """
        base = (base_username or f"tg_{telegram_id}").strip()[:24]
        desired = f"{base}_{invoice_id}" if invoice_id is not None else base
        data = await self._request('GET', '/api/users',
                                   params={"search": desired, "page": 1, "size": 10})
        if any(u.get('username') == desired for u in data.get('users', [])):
            desired = f"{desired}x{secrets.token_hex(2)}"

        final_password = password or secrets.token_urlsafe(16)
        resp = await self._request('POST', '/api/users/add', json_body={
            "username": desired,
            "password": final_password,
            "role": "user",
            "telegramId": str(telegram_id),
            "email": None,
            "description": f"VpnBot-m service (invoice {invoice_id})",
            "traffic_limit": 0,
            "traffic_reset_strategy": "never",
            "expiration_date": None,
        })
        self._check_success(resp, f"create service account {desired}")
        uid = resp.get('user_id')
        if not uid:
            raise AmneziaError(f"user/add returned no user_id: {resp}")
        logger.info(f"amnezia: created service account {desired} ({uid}) for tg {telegram_id}")
        return {"panel_user_id": uid, "username": desired, "password": final_password}

    async def update_user_limits(self, panel_user_id: str, telegram_id: int,
                                 quota_gb: int = None, expiry: datetime = None):
        """Set panel-user-level traffic limit (whole GB) and/or expiry date.

        NOTE: the panel multiplies traffic_limit by GiB internally; send GB,
        never bytes (verified live — byte values become astronomically large).
        """
        payload = {
            "telegramId": str(telegram_id),
            "email": None,
            "description": "created by VpnBot-m",
            "traffic_reset_strategy": "never",
            "password": None,
        }
        if quota_gb is not None:
            payload["traffic_limit"] = int(quota_gb)
        if expiry is not None:
            payload["expiration_date"] = format_expiration(expiry)
        # User writes are applied on the remote VPN server over SSH and can
        # block for minutes when the panel↔server link is slow (measured live).
        resp = await self._request('POST', f'/api/users/{panel_user_id}/update',
                                   json_body=payload, timeout=300.0)
        self._check_success(resp, f"update user limits for {panel_user_id}")

    async def set_user_enabled(self, panel_user_id: str, enabled: bool):
        # Measured live at ~145s: the panel applies the change on the remote VPN
        # server over SSH, so this needs a far larger timeout than the default.
        resp = await self._request('POST', f'/api/users/{panel_user_id}/toggle',
                                   json_body={"enabled": bool(enabled)},
                                   timeout=300.0)
        self._check_success(resp, f"toggle user {panel_user_id}")

    async def delete_panel_user(self, panel_user_id: str):
        """Delete the panel user AND all their connections (cascade, verified)."""
        resp = await self._request('POST', f'/api/users/{panel_user_id}/delete')
        self._check_success(resp, f"delete user {panel_user_id}")

    # ---------- usage stats ----------

    async def get_user_stats(self, panel_username: str) -> dict | None:
        """Fetch usage/expiry for a panel user by exact username match.

        Returns {"enabled", "used", "total", "limit", "expiration_date"(dt|None),
                 "connections_count"} with byte values, or None if not found.
        """
        data = await self._request('GET', '/api/users',
                                   params={"search": panel_username,
                                           "page": 1, "size": 10})
        for u in data.get('users', []):
            if u.get('username') == panel_username:
                return {
                    "enabled": u.get('enabled', False),
                    "used": u.get('traffic_used') or 0,
                    "total": u.get('traffic_total') or 0,
                    "limit": u.get('traffic_limit') or 0,
                    "expiration_date": parse_expiration(u.get('expiration_date')),
                    "connections_count": u.get('connections_count') or 0,
                }
        return None

    # ---------- connections ----------

    async def add_connection(self, panel_user_id: str, server_id: int,
                             name: str) -> dict:
        """Add a connection under a panel user.

        The add-response carries config + vpn_link but NOT the client/
        connection ids, so the user's connection list is fetched afterwards
        and matched by exact (unique) name.

        Returns {"connection_id", "client_id", "server_id", "protocol",
                 "config", "vpn_link", "server_name"}.
        """
        resp = await self._request(
            'POST', f'/api/users/{panel_user_id}/connections/add',
            json_body={
                "server_id": int(server_id),
                "protocol": self.protocol,
                "name": name,
                "client_id": None,
            })
        self._check_success(resp, f"add connection {name}")
        config = resp.get('config')
        vpn_link = resp.get('vpn_link')

        conns = await self.get_user_connections(panel_user_id)
        match = None
        for c in conns:
            if c.get('name') == name:
                match = c
        if match is None:
            raise AmneziaError(f"connection {name!r} added but not found in user listing")
        await self.remember_server_name_async(match.get('server_id'), match.get('server_name'))
        return {
            "connection_id": match['id'],
            "client_id": match['client_id'],
            "server_id": match.get('server_id', server_id),
            "protocol": match.get('protocol', self.protocol),
            "server_name": match.get('server_name'),
            "config": config,
            "vpn_link": vpn_link,
        }

    async def get_user_connections(self, panel_user_id: str) -> list[dict]:
        data = await self._request('GET', f'/api/users/{panel_user_id}/connections')
        return data.get('connections', [])

    async def get_connection_config(self, server_id: int,
                                    client_id: str) -> dict:
        """Return {"config", "vpn_link"} for one connection."""
        resp = await self._request(
            'POST', f'/api/servers/{int(server_id)}/connections/config',
            json_body={"protocol": self.protocol, "client_id": client_id})
        if not isinstance(resp, dict) or 'config' not in resp:
            raise AmneziaError(f"config fetch failed for client {client_id}: {resp}")
        return {"config": resp.get('config'), "vpn_link": resp.get('vpn_link')}

    async def remove_connection(self, server_id: int, client_id: str):
        resp = await self._request(
            'POST', f'/api/servers/{int(server_id)}/connections/remove',
            json_body={"protocol": self.protocol, "client_id": client_id})
        self._check_success(resp, f"remove connection {client_id}")
