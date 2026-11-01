# -*- coding: utf-8 -*-
"""
데이터 수집기 기본 클래스
"""
import logging
import asyncio
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timedelta
import pandas as pd
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)

_NIGHT_DERIVATIVE_MARKETS = {"CM", "EU"}


def select_derivative_session_rows(
    rows: List[Dict],
    target_date: str,
    market: str,
    session_date: str | None = None,
) -> Tuple[List[Dict], str | None]:
    """Select the KIS session corresponding to a KRX business date.

    KIS night-session clocks can run through 30:00 and rows are labelled by
    the evening session-start date. CM/EU callers pass that preceding KRX
    session explicitly; day markets default to the target business date.
    """
    if session_date is None:
        session_date = target_date

    if not session_date:
        return [], None
    return (
        [row for row in rows if str(row.get("stck_bsop_date", target_date)) == session_date],
        session_date,
    )


class BaseFetcher(ABC):
    """데이터 수집기 기본 클래스"""

    def __init__(self, api_client, **params):
        """
        Initialize base fetcher

        Args:
            api_client: RestApi instance
            **params: Additional parameters for specific fetcher
        """
        self.api_client = api_client
        self.params = params

        # Common parameters
        self.tr_id = params.get('tr_id')
        self.max_retries = params.get('max_retries', 5)
        self.retry_delay = params.get('retry_delay', 1.0)

        # Validate required parameters
        self._validate_params()

    @abstractmethod
    def _validate_params(self):
        """Validate required parameters for specific fetcher"""
        pass

    @abstractmethod
    async def fetch_data(self) -> Any:
        """Fetch data from API"""
        pass

    async def _request_with_retry(self, tr_id: str, params: Dict, method: str = "GET") -> Optional[Dict]:
        """
        Make API request with retry logic. Rate-limit errors (EGW00201) get
        exponential backoff; other errors use linear backoff.
        """
        for attempt in range(self.max_retries):
            try:
                response = await self.api_client.request_data(tr_id, params, method)

                if response and response.get('rt_cd') == '0':
                    return response

                error_code = response.get('msg_cd', '') if response else ''
                error_msg = response.get('msg1', 'Unknown error') if response else 'No response'
                logger.warning(f"API error (attempt {attempt + 1}/{self.max_retries}): {error_msg}")

                # EGW00201 = rate limit exceeded. Back off aggressively and
                # cap so we don't sit for minutes on repeated hits.
                # (token bucket at the client level should prevent this, but
                # if the account enters a penalty cooldown we give it room.)
                if error_code == 'EGW00201':
                    if attempt < self.max_retries - 1:
                        backoff = min(2.0 * (2 ** attempt), 30.0)  # 2, 4, 8, 16, 30s
                        await asyncio.sleep(backoff)
                        continue

            except Exception as e:
                logger.error(f"Request failed (attempt {attempt + 1}/{self.max_retries}): {e}")

            if attempt < self.max_retries - 1:
                await asyncio.sleep(self.retry_delay * (attempt + 1))

        logger.error(f"All {self.max_retries} attempts failed for TR_ID: {tr_id}")
        return None

    def _process_dataframe(self, df: pd.DataFrame, numeric_columns: List[str] = None) -> pd.DataFrame:
        """Process and clean dataframe"""
        if df.empty:
            return df

        if numeric_columns:
            for col in numeric_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')

        df = df.drop_duplicates()
        return df

    def _log_success(self, message: str):
        logger.info(f"[OK] {message}")

    def _log_warning(self, message: str):
        logger.warning(f"[WARN] {message}")

    def _log_error(self, message: str):
        logger.error(f"[ERR] {message}")

    def _log_info(self, message: str):
        logger.info(f"[INFO] {message}")
