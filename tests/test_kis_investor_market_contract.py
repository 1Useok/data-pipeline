"""2026-01 KIS investor-market collection contract."""


def test_january_investor_market_contract_is_complete():
    import kis_investor_trading as mod

    actual = {
        item["name"]: (item["code"], item["index_code"])
        for item in mod.MARKETS
    }
    assert actual == {
        "kospi": ("KSP", "0001"),
        "kosdaq": ("KSQ", "1001"),
        "futures": ("K2I", "F001"),
        "call_option": ("K2I", "OC01"),
        "put_option": ("K2I", "OP01"),
        "mini_futures": ("MKI", "F004"),
        "mini_call_option": ("MKI", "OC02"),
        "mini_put_option": ("MKI", "OP02"),
        "kosdaq150_futures": ("KQI", "F002"),
        "kosdaq150_call_option": ("KQI", "OC03"),
        "kosdaq150_put_option": ("KQI", "OP03"),
    }


def test_investor_market_endpoint_matches_official_api():
    import kis_investor_trading as mod

    endpoint = mod.load_config()["kis_api_endpoints"][mod.TR_ID]
    assert endpoint == (
        "/uapi/domestic-stock/v1/quotations/"
        "inquire-investor-time-by-market"
    )
