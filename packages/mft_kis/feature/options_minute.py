"""KIS API options minute bar fetcher."""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd

from mft_common.krx_calendar import previous_krx_session_date

from .base_fetcher import BaseFetcher, select_derivative_session_rows

logger = logging.getLogger(__name__)


class OptionsMinuteFetcher(BaseFetcher):
    """Fetch options minute bars from KIS API.

    TR_ID: FHKIF03020200 (same as futures)
    Market: O (index options), JO (stock options)
    """

    def __init__(self, api_client, **params):
        self.ticker = params.get("ticker")
        self.date = params.get("date")  # YYYYMMDD
        self.kis_market = params.get("kis_market", "O")
        if not params.get("tr_id"):
            params["tr_id"] = "FHKIF03020200"
        super().__init__(api_client, **params)

    def _validate_params(self):
        if not all([self.tr_id, self.ticker, self.date]):
            raise ValueError("Required: tr_id, ticker, date")

    async def fetch_data(self) -> Optional[pd.DataFrame]:
        """Fetch all minute bars for a single option on a single date."""
        next_date = (
            datetime.strptime(self.date, "%Y%m%d") + timedelta(days=1)
        ).strftime("%Y%m%d")
        if self.kis_market in {"CM", "EU"}:
            session_start = previous_krx_session_date(self.date).strftime("%Y%m%d")
            frames = []
            for query_date, input_hour in (
                (session_start, "235959"),
                (self.date, "153000"),
            ):
                frame = await self._fetch_data_for_query_date(
                    query_date, input_hour, session_start
                )
                if frame is None:
                    return None
                if not frame.empty:
                    frames.append(frame)
            if not frames:
                return pd.DataFrame()
            return pd.concat(frames, ignore_index=True).drop_duplicates()

        frame = await self._fetch_data_for_query_date(self.date)
        if frame is not None and frame.empty:
            frame = await self._fetch_data_for_query_date(next_date)
        return frame

    async def _fetch_data_for_query_date(
        self,
        query_date: str,
        input_hour: str = "153000",
        session_date: str | None = None,
    ) -> Optional[pd.DataFrame]:
        """Query one KIS boundary date while retaining only the target date."""
        params = {
            "FID_COND_MRKT_DIV_CODE": self.kis_market,
            "FID_INPUT_ISCD": self.ticker,
            "FID_INPUT_DATE_1": query_date,
            "FID_INPUT_HOUR_1": input_hour,
            "FID_HOUR_CLS_CODE": "60",
            "FID_PW_DATA_INCU_YN": "Y",
            "FID_FAKE_TICK_INCU_YN": "N",
        }

        all_rows = []
        response = await self._request_with_retry(self.tr_id, params)

        # None == real fetch failure; empty DataFrame == "no data on this
        # date" (common for OTM options that didn't trade). The runner's
        # circuit breaker only counts None as failure.
        if not response:
            return None

        output = response.get("output2", [])
        if not output:
            return pd.DataFrame()

        # A KRX night session is split across the preceding session-start
        # boundary and the target-day boundary. Its stck_bsop_date remains
        # the preceding start date. Filter every page to that date so
        # pagination cannot roll into an adjacent session.
        output, session_date = select_derivative_session_rows(
            output, self.date, self.kis_market, session_date
        )
        if not output:
            return pd.DataFrame()
        all_rows.extend(output)

        # FHKIF03020200 page size is 102 (vs the legacy hardcoded 30 here),
        # so paginate purely on whether KIS keeps progressing — track
        # last_time and stop when it no longer strictly decreases or when
        # KIS returns empty/wrong-day rows.
        prev_last_time = None
        while len(output) >= 102:
            last_time = output[-1].get("stck_cntg_hour", "")
            if not last_time:
                break
            if prev_last_time is not None and last_time >= prev_last_time:
                break
            prev_last_time = last_time
            params["FID_INPUT_DATE_1"] = session_date
            params["FID_INPUT_HOUR_1"] = last_time

            response = await self._request_with_retry(self.tr_id, params)
            if not response:
                break
            output = response.get("output2", [])
            if not output:
                break

            same_day, _ = select_derivative_session_rows(
                output, self.date, self.kis_market, session_date
            )
            all_rows.extend(same_day)
            if len(same_day) < len(output):
                break
            output = same_day

        if not all_rows:
            return pd.DataFrame()

        df = pd.DataFrame(all_rows)
        numeric_cols = ["futs_prpr", "futs_oprc", "futs_hgpr", "futs_lwpr", "cntg_vol", "acml_tr_pbmn"]
        df = self._process_dataframe(df, numeric_cols)
        if "stck_bsop_date" not in df.columns:
            df["stck_bsop_date"] = session_date or self.date

        return df
