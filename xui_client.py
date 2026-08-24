import os
import asyncio
import httpx


class ClientNotFoundError(Exception):
    """Raised when the panel definitively reports a client does not exist
    (HTTP 200 with success:false, msg "record not found"). This is a valid
    negative answer, NOT a transient failure, so _request never retries it —
    retrying wasted ~3.5s of backoff per missing-client lookup."""
    pass


# Global singleton instance for XUIClient
_xui_client_instance = None


def get_xui_client() -> 'XUIClient':
    """Get or create the global singleton XUIClient instance.

    This function returns a shared XUIClient instance that maintains
    a persistent HTTP session and authentication state across multiple
    requests, significantly improving performance by avoiding repeated
    authentication overhead.
    """
    global _xui_client_instance
    if _xui_client_instance is None:
        _xui_client_instance = XUIClient()
    return _xui_client_instance


class XUIClient:
    """
    3x-ui API client using session-based authentication (cookie + CSRF token).

    Auth flow:
    1. GET / → captures session cookie
    2. GET /csrf-token → retrieves fresh CSRF token
    3. POST /login → authenticates with username/password + CSRF token
    4. All subsequent requests reuse the session cookie;
       state-changing methods (POST/PUT/DELETE) first refresh the CSRF token.
    """

    def __init__(self):
        self.base_url = os.getenv('PANEL_URL').rstrip('/')
        self.username = os.getenv('PANEL_USERNAME')
        self.password = os.getenv('PANEL_PASSWORD')
        verify_ssl = os.getenv('PANEL_SSL_VERIFY', 'false').lower() == 'true'
        self._client = httpx.AsyncClient(verify=verify_ssl, timeout=30.0)
        self._csrf_token = None
        self._authenticated = False

    async def _ensure_authenticated(self):
        """Login to the panel if not already authenticated."""
        if self._authenticated:
            return

        # 1) Get initial page → captures session cookie in the client's cookie jar
        r = await self._client.get(self.base_url + '/')
        r.raise_for_status()

        # 2) Fetch a fresh CSRF token
        csrf_data = await self._fetch_csrf_token()
        self._csrf_token = csrf_data

        # 3) Login with credentials
        login_resp = await self._client.post(
            self.base_url + '/login',
            data={"username": self.username, "password": self.password},
            headers={
                "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                "X-Requested-With": "XMLHttpRequest",
                "X-CSRF-Token": self._csrf_token,
            }
        )
        login_resp.raise_for_status()
        login_json = login_resp.json()
        if not login_json.get('success'):
            raise Exception(f"Panel login failed: {login_json.get('msg', 'unknown error')}")

        self._authenticated = True

    async def _fetch_csrf_token(self) -> str:
        """GET /csrf-token and return the token string."""
        r = await self._client.get(
            self.base_url + '/csrf-token',
            headers={"X-Requested-With": "XMLHttpRequest"},
        )
        r.raise_for_status()
        return r.json().get('obj', '')

    def _is_safe_method(self, method: str) -> bool:
        return method.upper() in ('GET', 'HEAD', 'OPTIONS', 'TRACE')

    async def _request(self, method: str, endpoint: str, **kwargs):
        """Core request method with retry logic — authenticates, refreshes CSRF, sends request."""
        max_retries = 3
        retry_delay = 0.5
        for attempt in range(max_retries + 1):
            try:
                await self._ensure_authenticated()

                is_safe = self._is_safe_method(method)
                if not is_safe:
                    # Refresh CSRF token before state-changing operations
                    self._csrf_token = await self._fetch_csrf_token()

                # Merge headers
                headers = kwargs.pop('headers', {})
                headers["X-Requested-With"] = "XMLHttpRequest"
                if not is_safe and self._csrf_token:
                    headers["X-CSRF-Token"] = self._csrf_token

                timeout = kwargs.pop('timeout', self._client.timeout)

                response = await self._client.request(
                    method,
                    f"{self.base_url}{endpoint}",
                    headers=headers,
                    timeout=timeout,
                    **kwargs
                )
                response.raise_for_status()
                result = response.json()
                if not result.get('success', True):
                    msg = result.get('msg', 'unknown error')
                    # A definitive "record not found" is a valid negative answer
                    # (HTTP 200, success:false), not a transient failure. Retrying
                    # it just burns 3.5s of backoff per lookup — that is what made
                    # "My Services" and the new-client pre-check slow. Fail fast.
                    if "record not found" in msg.lower():
                        raise ClientNotFoundError(msg)
                    raise Exception(f"Panel API error: {msg}")
                return result
            except ClientNotFoundError:
                raise  # definitive negative — never retry
            except Exception as e:
                # A 401/403 means the panel invalidated our session or CSRF token
                # (e.g. panel restart, token rotation) *after* we last logged in.
                # self._authenticated stays True across this exception, so without
                # resetting it here, every retry would keep resending the same
                # dead cookie and just 401 again until retries run out — which is
                # exactly what made every service show up as "unknown" (❓) in
                # "My Services" during a single stale-session event. Force the
                # next attempt to log in again instead of trusting the cache.
                is_auth_error = (
                    isinstance(e, httpx.HTTPStatusError)
                    and e.response.status_code in (401, 403)
                )
                if is_auth_error:
                    self._authenticated = False
                    self._csrf_token = None
                if attempt < max_retries:
                    # Exponential backoff
                    wait_time = retry_delay * (2 ** attempt)
                    await asyncio.sleep(wait_time)
                else:
                    raise
        # Should not reach here
        raise Exception("Request failed after retries")

    async def close(self):
        """Close the underlying HTTP client session."""
        await self._client.aclose()

    # ------------------------------------------------------------------
    # Public API methods
    # ------------------------------------------------------------------

    async def get_inbounds(self):
        res = await self._request("GET", "/panel/api/inbounds/list")
        return res.get('obj', [])

    async def get_enabled_inbounds(self):
        """Return only inbounds that are enabled (enable == True)."""
        inbounds = await self.get_inbounds()
        return [ib for ib in inbounds if ib.get('enable', False)]

    async def add_client(self, email: str, total_bytes: int, expiry_time: int,
                         inbound_ids: list, tg_id=None, sub_id=None):
        client = {"email": email, "totalGB": total_bytes,
                  "expiryTime": expiry_time, "enable": True}
        if tg_id:
            client["tgId"] = tg_id
        if sub_id:
            client["subId"] = sub_id
        payload = {"client": client, "inboundIds": inbound_ids}
        return await self._request("POST", "/panel/api/clients/add", json=payload)

    async def assign_group(self, email: str, tg_id: str):
        payload = {"emails": [email], "group": str(tg_id)}
        return await self._request("POST", "/panel/api/clients/groups/bulkAdd", json=payload)

    async def get_client_stats(self, email: str):
        res = await self._request("GET", f"/panel/api/clients/traffic/{email}")
        return res.get('obj', {})

    async def get_client_full(self, email: str):
        try:
            res = await self._request("GET", f"/panel/api/clients/get/{email}")
            return res.get('obj', {})
        except ClientNotFoundError:
            return None
        except Exception as e:
            # Backward-compat: some panel builds phrase the not-found differently.
            if "record not found" in str(e).lower():
                return None
            raise

    async def update_client(self, email: str, payload: dict):
        return await self._request("POST", f"/panel/api/clients/update/{email}", json=payload)

    async def reset_client_traffic(self, email: str):
        return await self._request("POST", f"/panel/api/clients/resetTraffic/{email}")

    async def get_group_emails(self, tg_id: str):
        try:
            res = await self._request("GET", f"/panel/api/clients/groups/{tg_id}/emails")
            return res.get('obj', [])
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                return []
            raise

    # ======= ADVANCED CAPABILITIES =======
    async def get_client_links(self, email: str):
        try:
            res = await self._request("GET", f"/panel/api/clients/links/{email}")
            return res.get('obj', [])
        except Exception:
            return []

    async def get_client_ips(self, email: str):
        res = await self._request("POST", f"/panel/api/clients/ips/{email}")
        return res.get('obj', [])

    async def clear_client_ips(self, email: str):
        return await self._request("POST", f"/panel/api/clients/clearIps/{email}")

    async def get_last_online(self):
        res = await self._request("POST", "/panel/api/clients/lastOnline")
        return res.get('obj', {})

    async def get_server_status(self):
        res = await self._request("GET", "/panel/api/server/status")
        return res.get('obj', {})

    async def delete_depleted_clients(self):
        res = await self._request("POST", "/panel/api/clients/delDepleted")
        return res.get('obj', {})

    async def delete_client(self, email: str, keep_traffic: bool = False):
        """keep_traffic=True preserves the client's usage counters — required
        before a delete/re-create inbound-membership fix so users don't lose
        their traffic statistics."""
        return await self._request(
            "POST", f"/panel/api/clients/del/{email}?keepTraffic={1 if keep_traffic else 0}")

    async def restart_xray(self):
        return await self._request("POST", "/panel/api/server/restartXrayService")

    async def restart_panel(self):
        try:
            await self._request("POST", "/panel/setting/restartPanel", timeout=2.0)
        except (httpx.ReadTimeout, httpx.ConnectError):
            pass  # Panel drops connection on restart — expected
        return True

    async def backup_to_telegram(self):
        return await self._request("POST", "/panel/api/backuptotgbot")
