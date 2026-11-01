"""KIS API stock/ETF/ELW minute bar fetcher."""

import asyncio
import logging
from typing import Optional

import pandas as pd

from .base_fetcher import BaseFetcher

logger = logging.getLogger(__name__)


class StockMinuteFetcher(BaseFetcher):
    """Fetch stock/ETF/ELW minute bars from KIS API.

    TR_ID: FHKST03010230 (주식일별분봉조회 — supports historical dates).
    The sibling TR_ID FHKST03010200 (주식당일분봉조회) only ever returns the
    current trading day regardless of FID_INPUT_DATE_1, which silently
    returned wrong-date data and broke pagination. Use 230 for all queries.
    Market: J (KRX)
    """

    def __init__(self, api_client, **params):
        self.ticker = params.get("ticker")
        self.date = params.get("date")  # YYYYMMDD
        # KIS market_div_code:
        #   "UN" - KRX + NXT unified (recommended default for stocks/ETFs;
        #          captures NXT alternative-venue trades to match koscom
        #          pcap-derived volumes).
        #   "J"  - KRX only. Use for instruments with no NXT trading
        #          (KRG5* ELW, KRG6* ETN, foreign equities).
        #   "W"  - KRA5* foreign-underlying ELW.
        # Caller picks via the universe entry's kis_market field; default
        # "UN" is safe for non-NXT instruments too (returns same as J).
        self.kis_market = params.get("kis_market", "UN")
        if not params.get("tr_id"):
            params["tr_id"] = "FHKST03010230"
        super().__init__(api_client, **params)

    def _validate_params(self):
        if not all([self.tr_id, self.ticker, self.date]):
            raise ValueError("Required: tr_id, ticker, date")

    async def fetch_data(self) -> Optional[pd.DataFrame]:
        """Fetch all minute bars for a single stock on a single date."""
        frame = await self._fetch_data_for_market(self.kis_market)
        if frame is not None and frame.empty and self.kis_market == "UN":
            logger.info(
                "Unified market returned no rows for %s on %s; retrying KRX market",
                self.ticker,
                self.date,
            )
            frame = await self._fetch_data_for_market("J")
        return frame

    async def _fetch_data_for_market(self, market: str) -> Optional[pd.DataFrame]:
        """Fetch one market code; callers may fall back from UN to J."""
        params = {
            "FID_COND_MRKT_DIV_CODE": market,
            "FID_INPUT_ISCD": self.ticker,
            "FID_INPUT_HOUR_1": "153000",
            "FID_INPUT_DATE_1": self.date,
            "FID_PW_DATA_INCU_YN": "Y",
            # FHKST03010230 requires this field; value must be a single
            # space (not "N"), per KIS spec for "허봉 포함 여부".
            "FID_FAKE_TICK_INCU_YN": " ",
        }

        all_rows = []
        response = await self._request_with_retry(self.tr_id, params)

        # None == real fetch failure (HTTP/retry exhausted). Empty DataFrame
        # == "no data on this date" (e.g. halted, SPAC, illiquid stocks
        # that had no trades). The runner distinguishes these — None
        # increments the failure counter and trips the circuit breaker;
        # empty DataFrame is treated as a quiet success.
        if not response:
            return None

        output = response.get("output2", [])
        if not output:
            return pd.DataFrame()

        # FHKST03010230 still rolls into prior trading days once we paginate
        # past 09:00. Filter every page by stck_bsop_date and stop on the
        # first wrong-date row. If KIS returned only prior-day rows (e.g.
        # SPACs that haven't traded since), filtering empties the page —
        # that's "no data", not failure.
        output = [r for r in output if r.get("stck_bsop_date", self.date) == self.date]
        if not output:
            return pd.DataFrame()
        all_rows.extend(output)

        # Page size for FHKST03010230 is 120 rows. Paginate while the page
        # was full AND last_time strictly decreases (defensive against
        # KIS returning the same page twice).
        prev_last_time = None
        while len(output) >= 120:
            last_time = output[-1].get("stck_cntg_hour", "")
            if not last_time:
                break
            if prev_last_time is not None and last_time >= prev_last_time:
                break
            prev_last_time = last_time
            params["FID_INPUT_HOUR_1"] = last_time

            response = await self._request_with_retry(self.tr_id, params)
            if not response:
                break
            output = response.get("output2", [])
            if not output:
                break

            same_day = [r for r in output if r.get("stck_bsop_date", self.date) == self.date]
            all_rows.extend(same_day)
            if len(same_day) < len(output):
                break
            output = same_day

        if not all_rows:
            return pd.DataFrame()

        df = pd.DataFrame(all_rows)
        numeric_cols = ["stck_prpr", "stck_oprc", "stck_hgpr", "stck_lwpr", "cntg_vol", "acml_tr_pbmn"]
        df = self._process_dataframe(df, numeric_cols)
        df["stck_bsop_date"] = self.date

        return df
