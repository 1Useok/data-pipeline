"""
Continuous Future Product Definitions.

Maps product names to ISIN prefixes for Korean futures markets.
"""

from dataclasses import dataclass
from typing import Dict, List, Optional


@dataclass(frozen=True)
class FutureProduct:
    """Definition of a continuous future product."""

    # Product identifier used in sec_code (e.g., "kospi_future")
    sec_code: str

    # ISIN prefix to filter contracts (e.g., "KR4101" for KOSPI200)
    isin_prefix: str

    # Human-readable name
    name: str

    # Description
    description: str = ""


# Product definitions matching existing continuous_future data
# Note: Uses "usd_future" (not "dollar_future") to match existing data
PRODUCTS: Dict[str, FutureProduct] = {
    "kospi": FutureProduct(
        sec_code="kospi_future",
        isin_prefix="KR4A01",
        name="KOSPI200 Futures",
        description="KOSPI200 Index Futures",
    ),
    "kosdaq": FutureProduct(
        sec_code="kosdaq_future",
        isin_prefix="KR4A06",
        name="KOSDAQ150 Futures",
        description="KOSDAQ150 Index Futures",
    ),
    "usd": FutureProduct(
        sec_code="usd_future",
        isin_prefix="KR4A75",
        name="USD/KRW Futures",
        description="US Dollar Currency Futures",
    ),
    "ktb10": FutureProduct(
        sec_code="ktb10_future",
        isin_prefix="KR4A67",
        name="KTB 10Y Futures",
        description="Korea Treasury Bond 10-Year Futures",
    ),
}

# Additional products that could be added in the future
PRODUCTS_EXTENDED: Dict[str, FutureProduct] = {
    "ktb3": FutureProduct(
        sec_code="ktb3_future",
        isin_prefix="KR4A65",
        name="KTB 3Y Futures",
        description="Korea Treasury Bond 3-Year Futures",
    ),
}


def get_product(key: str) -> FutureProduct:
    """Get product by key."""
    if key not in PRODUCTS:
        raise ValueError(f"Unknown product: {key}. Available: {list(PRODUCTS.keys())}")
    return PRODUCTS[key]


def get_all_products() -> List[FutureProduct]:
    """Get all defined products."""
    return list(PRODUCTS.values())


def get_product_by_sec_code(sec_code: str) -> Optional[FutureProduct]:
    """Get product by sec_code (e.g., 'kospi_future')."""
    for product in PRODUCTS.values():
        if product.sec_code == sec_code:
            return product
    return None


def get_isin_prefix_map() -> Dict[str, str]:
    """Get mapping of ISIN prefix to sec_code."""
    return {p.isin_prefix: p.sec_code for p in PRODUCTS.values()}
