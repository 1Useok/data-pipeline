from __future__ import annotations

import pandas as pd

from mft_krx.crawlers import core


def test_bulk_investor_collection_keeps_unranked_symbols_with_zero(monkeypatch):
    universe = pd.DataFrame(
        {
            "date": [pd.Timestamp("2026-07-31")] * 3,
            "sec_code": ["KR0000000001", "KR0000000002", "KR0000000003"],
            "종목코드": ["000001", "000002", "000003"],
            "종목명": ["첫째", "둘째", "코넥스"],
            "시장구분": ["KOSPI", "KOSDAQ", "KONEX"],
        }
    )
    monkeypatch.setattr(core, "전종목시세_12001", lambda date: universe.copy())

    def fake_investor(date, investor_name, investor_code):
        return pd.DataFrame({"종목코드": ["000001"], investor_name: [7]})

    monkeypatch.setattr(core, "_투자자별순매수종목", fake_investor)
    monkeypatch.setattr(core.time, "sleep", lambda seconds: None)

    result = core.투자자별거래실적개별종목_12010("2026-07-31")

    assert result["종목코드"].tolist() == ["000001", "000002"]
    assert result.loc[result["종목코드"] == "000001", "금융투자"].item() == 7
    assert result.loc[result["종목코드"] == "000002", "금융투자"].item() == 0
    assert set(core.INVESTOR_TYPE_CODES).issubset(result.columns)


def test_bulk_investor_response_is_converted_to_krw_millions(monkeypatch):
    monkeypatch.setattr(
        core,
        "_request_data_json",
        lambda *args, **kwargs: pd.DataFrame(
            {
                "종목코드": ["5930", "000660"],
                "NETBID_TRDVAL": ["1,999,999", "-2,999,999"],
            }
        ),
    )

    result = core._투자자별순매수종목(
        pd.Timestamp("2026-07-31"), "금융투자", "1000"
    )

    assert result.to_dict("records") == [
        {"종목코드": "005930", "금융투자": 1},
        {"종목코드": "000660", "금융투자": -2},
    ]
