# -*- coding: utf-8 -*-
"""
KIS OpenAPI REST 클라이언트 (데이터 수집 전용)
"""
import logging
import asyncio
import os
import aiohttp
from typing import Dict, Optional, Any

from aiolimiter import AsyncLimiter

from .token_manager import KISTokenManager

logger = logging.getLogger(__name__)

# KIS prod account rate limit. Conservative setting for unattended nightly
# batches — total runtime ~3-4 hours for the full universe is acceptable;
# the goal is NO EGW00201 errors at all. The daily default is 3 req/sec;
# an explicitly configured historical recovery may use a higher rate. KIS's
# official kis_auth.py sample uses 0.05s for prod and 0.5s for paper. Burst is
# still one, and all RestApi instances in the process share this limiter.
try:
    _KIS_REQUESTS_PER_SECOND = max(
        0.1, float(os.environ.get("KIS_REQUESTS_PER_SECOND", "3"))
    )
except (TypeError, ValueError):
    _KIS_REQUESTS_PER_SECOND = 3.0
_KIS_RATE_LIMIT = AsyncLimiter(
    max_rate=1,
    time_period=1.0 / _KIS_REQUESTS_PER_SECOND,
)


def _float_env(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, default))
    except (TypeError, ValueError):
        return default


def _client_timeout() -> aiohttp.ClientTimeout:
    return aiohttp.ClientTimeout(
        total=_float_env("KIS_HTTP_TOTAL_TIMEOUT", 30.0),
        connect=_float_env("KIS_HTTP_CONNECT_TIMEOUT", 5.0),
        sock_connect=_float_env("KIS_HTTP_SOCK_CONNECT_TIMEOUT", 5.0),
        sock_read=_float_env("KIS_HTTP_SOCK_READ_TIMEOUT", 20.0),
    )


class RestApi:
    """한국투자증권 REST API 클라이언트 (데이터 수집 전용)"""

    def __init__(self, config: dict, context: str = 'data'):
        """
        RestApi 초기화

        Args:
            config: 설정 딕셔너리
            context: 사용 컨텍스트 ('data' 전용)
        """
        self.context = context
        self.config = config

        # 시세/데이터 -> PROD (실전투자) 사용
        api_config = config.get('prod_api', {})

        # API 설정 추출
        self.app_key = api_config.get('app_key')
        self.app_secret = api_config.get('app_secret')
        self.base_url = api_config.get('base_url', 'https://openapi.koreainvestment.com:9443')

        self._session: Optional[aiohttp.ClientSession] = None

        # KIS API 엔드포인트 매핑
        self.endpoints = config.get('kis_api_endpoints', {})

        # TokenManager 초기화
        self.token_manager = KISTokenManager(svr='prod')
        self.token_manager.configure(api_config)

        logger.info(f"RestApi initialized for data collection")

    async def is_connected(self) -> bool:
        """API 연결 상태 확인 및 세션 초기화"""
        if not self._session:
            self._session = aiohttp.ClientSession(timeout=_client_timeout())
        try:
            token = await self.token_manager.get_token()
            if token:
                logger.info("REST API connection established")
                return True
            return False
        except Exception as e:
            logger.exception(f"Failed to establish REST API connection: {e}")
            return False

    async def get_access_token(self) -> str:
        """TokenManager를 통해 토큰 반환"""
        return await self.token_manager.get_token()

    async def request_data(self, tr_id: str, params: Dict[str, Any], method: str = "GET") -> Dict[str, Any]:
        """
        API 데이터 요청

        Args:
            tr_id: 거래 ID (예: FHPTJ04030000)
            params: 요청 파라미터
            method: HTTP 메서드 (GET/POST)

        Returns:
            API 응답 딕셔너리
        """
        if not self._session:
            await self.is_connected()

        access_token = await self.token_manager.get_token()

        headers = {
            "authorization": f"Bearer {access_token}",
            "appkey": self.app_key,
            "appsecret": self.app_secret,
            "tr_id": tr_id
        }

        # TR_ID에 해당하는 엔드포인트 찾기
        endpoint = self.endpoints.get(tr_id)
        if not endpoint:
            raise ValueError(f"Unknown TR_ID: {tr_id}. Please add it to kis_api_endpoints in config.yaml")

        url = f"{self.base_url}{endpoint}"

        logger.debug(f"Request: TR_ID={tr_id}, Method={method}, URL={url}")
        try:
            # Global token-bucket: blocks until a slot is available.
            async with _KIS_RATE_LIMIT:
                if method.upper() == "GET":
                    async with self._session.get(url, headers=headers, params=params) as response:
                        result = await response.json()
                else:
                    async with self._session.post(url, headers=headers, json=params) as response:
                        result = await response.json()

            # API 에러 체크
            rt_cd = result.get('rt_cd', '')
            if rt_cd and rt_cd != '0':
                logger.error(f"API Error - TR_ID: {tr_id}, Code: {rt_cd}, Msg: {result.get('msg1')}")
            else:
                logger.debug(f"Response for TR_ID={tr_id}: Success")

            return result

        except aiohttp.ClientError as e:
            logger.error(f"Client error: {e}")
            raise
        except asyncio.TimeoutError:
            logger.error("Request timed out")
            raise
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            raise

    async def close(self):
        """리소스 정리"""
        if self._session:
            await self._session.close()
            self._session = None
        if self.token_manager:
            await self.token_manager.close()
