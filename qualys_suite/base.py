"""
base.py - Core Qualys API Session and Request Handler
Handles authentication, request routing, and XML/JSON parsing.
"""
import requests
import time
from requests.auth import HTTPBasicAuth
from typing import Optional, Dict, Any


class QualysSession:
    """
    Context manager and base class for all Qualys API calls.
    Handles Basic Auth, session tokens, and rate limiting.
    """

    def __init__(self, platform_url: str, username: str, password: str, use_session_auth: bool = False):
        """
        Args:
            platform_url: e.g. "https://qualysapi.qualys.com"
            username:     Your Qualys username.
            password:     Your Qualys password.
            use_session_auth: If True, logs in via /msp/session.php first.
        """
        self.base_url = platform_url.rstrip("/")
        self.username = username
        self.password = password
        self.use_session_auth = use_session_auth
        self._session = requests.Session()
        self._session.verify = True

    def __enter__(self):
        if self.use_session_auth:
            self._login()
        else:
            self._session.auth = HTTPBasicAuth(self.username, self.password)
        return self

    def __exit__(self, *args):
        if self.use_session_auth:
            self._logout()
        self._session.close()

    # ------------------------------------------------------------------
    # Auth
    # ------------------------------------------------------------------
    def _login(self):
        url = f"{self.base_url}/msp/session.php"
        resp = self._session.post(url, data={"action": "login", "username": self.username, "password": self.password})
        resp.raise_for_status()

    def _logout(self):
        url = f"{self.base_url}/msp/session.php"
        self._session.post(url, data={"action": "logout"})

    # ------------------------------------------------------------------
    # Internal Request Helpers
    # ------------------------------------------------------------------
    def _get(self, endpoint: str, params: Optional[Dict] = None, **kwargs) -> requests.Response:
        url = f"{self.base_url}{endpoint}"
        resp = self._session.get(url, params=params, **kwargs)
        resp.raise_for_status()
        return resp

    def _post(self, endpoint: str, data: Optional[Dict] = None, json: Optional[Dict] = None,
              headers: Optional[Dict] = None, **kwargs) -> requests.Response:
        url = f"{self.base_url}{endpoint}"
        resp = self._session.post(url, data=data, json=json, headers=headers, **kwargs)
        resp.raise_for_status()
        return resp

    # ------------------------------------------------------------------
    # Utility
    # ------------------------------------------------------------------
    def get_api_info(self) -> requests.Response:
        """Get basic API subscription/info."""
        return self._get("/msp/about.php")
