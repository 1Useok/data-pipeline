from __future__ import annotations

import os
import time
from dataclasses import dataclass, field
from typing import Optional

import requests


LOGIN_PAGE_URL = "https://data.krx.co.kr/contents/MDC/COMS/client/MDCCOMS001.cmd"
LOGIN_FORM_URL = "https://data.krx.co.kr/contents/MDC/COMS/client/view/login.jsp?site=mdc"
LOGIN_ACTION_URL = "https://data.krx.co.kr/contents/MDC/COMS/client/MDCCOMS001D1.cmd"
DEFAULT_REFERER = "https://data.krx.co.kr/contents/MDC/MDI/outerLoader/index.cmd"
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/131.0.0.0 Safari/537.36"
)


@dataclass
class KRXAuthSession:
    session: requests.Session = field(default_factory=requests.Session)
    login_time: float = 0.0
    expires_at: float = 0.0

    def is_valid(self) -> bool:
        return self.expires_at > time.time() + 300

    def login(self, login_id: str, password: str) -> None:
        self.session.close()
        self.session = requests.Session()

        warmup_headers = {"User-Agent": DEFAULT_USER_AGENT}
        self.session.get(LOGIN_PAGE_URL, headers=warmup_headers, timeout=15)
        self.session.get(
            LOGIN_FORM_URL,
            headers={**warmup_headers, "Referer": LOGIN_PAGE_URL},
            timeout=15,
        )

        payload = {
            "mbrNm": "",
            "telNo": "",
            "di": "",
            "certType": "",
            "mbrId": login_id,
            "pw": password,
        }
        data = self._post_login(payload)
        if data.get("_error_code") == "CD011":
            payload["skipDup"] = "Y"
            data = self._post_login(payload)

        if data.get("_error_code") != "CD001":
            message = data.get("_error_message") or "unknown KRX login error"
            code = data.get("_error_code") or "unknown"
            raise RuntimeError(f"KRX login failed ({code}): {message}")

        self.login_time = time.time()
        self.expires_at = self.login_time + 3600

    def _post_login(self, payload: dict[str, str]) -> dict:
        response = self.session.post(
            LOGIN_ACTION_URL,
            data=payload,
            headers={"User-Agent": DEFAULT_USER_AGENT, "Referer": LOGIN_PAGE_URL},
            timeout=15,
        )
        response.raise_for_status()
        return response.json()


_auth_session: Optional[KRXAuthSession] = None
_anonymous_session: Optional[requests.Session] = None


def get_krx_session() -> requests.Session:
    """Return an authenticated KRX session when credentials are configured."""
    global _auth_session, _anonymous_session

    login_id = os.getenv("KRX_ID")
    password = os.getenv("KRX_PW")
    if not (login_id and password):
        if _anonymous_session is None:
            _anonymous_session = requests.Session()
        return _anonymous_session

    if _auth_session is None:
        _auth_session = KRXAuthSession()

    if not _auth_session.is_valid():
        _auth_session.login(login_id, password)

    return _auth_session.session
