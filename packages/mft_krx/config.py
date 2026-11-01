"""
Configuration for KRX Crawler.

Provides unified configuration for KRX data crawling.
Uses Pydantic v2 for validation and type safety.
"""

import os
from pathlib import Path
from typing import Dict, List, Optional, Union

import yaml
from pydantic import BaseModel, Field, ConfigDict


# =============================================================================
# Crawl Configuration (HTTP/Network)
# =============================================================================

class HttpConfig(BaseModel):
    """HTTP endpoint configuration."""
    model_config = ConfigDict(frozen=False)

    gen_url: str = "https://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd"
    download_url: str = "https://data.krx.co.kr/comm/fileDn/download_csv/download.cmd"
    json_url: str = "https://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"
    referer: str = "https://data.krx.co.kr/contents/MDC/MDI/outerLoader/index.cmd"
    user_agent: str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"

    def get_header_otp(self) -> Dict[str, str]:
        """Get headers for OTP generation request."""
        return {
            "Accept": "text/plain, */*; q=0.01",
            "Accept-Encoding": "gzip, deflate",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "DNT": "1",
            "Host": "data.krx.co.kr",
            "Origin": "https://data.krx.co.kr",
            "Referer": self.referer,
            "User-Agent": self.user_agent,
            "X-Requested-With": "XMLHttpRequest",
        }

    def get_header_download(self) -> Dict[str, str]:
        """Get headers for CSV download request."""
        return {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
            "Accept-Encoding": "gzip, deflate",
            "Content-Type": "application/x-www-form-urlencoded",
            "DNT": "1",
            "Host": "data.krx.co.kr",
            "Origin": "https://data.krx.co.kr",
            "Referer": self.referer,
            "Upgrade-Insecure-Requests": "1",
            "User-Agent": self.user_agent,
        }

    def get_header_json(self, referer: Optional[str] = None) -> Dict[str, str]:
        """Get headers for JSON API request."""
        return {
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Accept-Encoding": "gzip, deflate",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "Host": "data.krx.co.kr",
            "Origin": "https://data.krx.co.kr",
            "Referer": referer or self.referer,
            "User-Agent": self.user_agent,
            "X-Requested-With": "XMLHttpRequest",
        }


class RetryConfig(BaseModel):
    """Retry configuration."""
    max_count: int = 30
    delay_seconds: float = 2.2


class ProxyConfig(BaseModel):
    """Proxy configuration."""
    model_config = ConfigDict(frozen=False)

    host: Optional[str] = None
    port: Optional[int] = None

    def get_proxy_url(self) -> Optional[str]:
        """Get proxy URL string."""
        if self.host and self.port:
            return f"{self.host}:{self.port}"
        return None


class CrawlConfig(BaseModel):
    """Crawl module configuration."""
    model_config = ConfigDict(frozen=False)

    http: HttpConfig = Field(default_factory=HttpConfig)
    retry: RetryConfig = Field(default_factory=RetryConfig)
    proxy: ProxyConfig = Field(default_factory=ProxyConfig)
    encoding: str = "cp949"


# =============================================================================
# Crawl Output Configuration (MFT_archive)
# =============================================================================

class PermissionsConfig(BaseModel):
    """File permissions configuration."""
    mode: Optional[str] = None
    group: Optional[str] = None


def _get_archive_base_dir(base_dir: Optional[str]) -> Path:
    """Get MFT_archive base directory with auto-detection fallback."""
    if env_path := os.environ.get("DATA_DB_PATH"):
        return Path(env_path) / "MFT_backtesting" / "KR"

    # Environment variable takes precedence
    if env_path := os.environ.get("MFT_ARCHIVE_PATH"):
        return Path(env_path)

    # Config file value
    if base_dir:
        return Path(base_dir)

    raise ValueError(
        "Data path is not configured. "
        "Set DATA_DB_PATH (or MFT_ARCHIVE_PATH) env var, or base_dir in config.yaml"
    )


def _get_raw_base_dir(base_dir: Optional[str]) -> Path:
    """Get MFT_raw base directory with auto-detection fallback."""
    if env_path := os.environ.get("DATA_DB_PATH"):
        return Path(env_path) / "MFT_raw"

    # Environment variable takes precedence
    if env_path := os.environ.get("MFT_RAW_PATH"):
        return Path(env_path)

    # Config file value
    if base_dir:
        return Path(base_dir)

    raise ValueError(
        "Data path is not configured. "
        "Set DATA_DB_PATH (or MFT_RAW_PATH) env var, or base_dir in config.yaml"
    )


class CrawlOutputConfig(BaseModel):
    """Crawl output path configuration (MFT_archive)."""
    model_config = ConfigDict(frozen=False)

    base_dir: Optional[str] = None
    country: str = "KR"
    subdirectory: str = "krx_crawled"
    file_pattern: str = "{date}.parquet"
    compression: str = "lz4"
    permissions: PermissionsConfig = Field(default_factory=PermissionsConfig)

    def get_base_dir(self) -> Path:
        """Get base directory with auto-detection fallback."""
        return _get_archive_base_dir(self.base_dir)

    def get_output_dir(self, item_name: str) -> Path:
        """Get output directory for a specific item."""
        return self.get_base_dir() / self.country / self.subdirectory / item_name

    def get_file_path(self, item_name: str, date_str: str) -> Path:
        """Get full file path for a specific item and date.

        Args:
            item_name: Crawl target name (e.g., "전종목시세")
            date_str: Date string in YYYY-MM-DD format

        Returns:
            Full path to the parquet file
        """
        filename = self.file_pattern.format(date=date_str)
        return self.get_output_dir(item_name) / filename


# Backward compatibility alias
OutputConfig = CrawlOutputConfig


# =============================================================================
# Convert Configuration (MFT_archive → MFT_raw)
# =============================================================================

class ConvertSourceConfig(BaseModel):
    """Convert source path configuration (MFT_archive)."""
    model_config = ConfigDict(frozen=False)

    base_dir: Optional[str] = None
    country: str = "KR"
    subdirectory: str = "krx_crawled"

    def get_base_dir(self) -> Path:
        """Get base directory with auto-detection fallback."""
        return _get_archive_base_dir(self.base_dir)

    def get_source_dir(self, item_name: str) -> Path:
        """Get source directory for a specific item."""
        return self.get_base_dir() / self.country / self.subdirectory / item_name

    def get_source_pattern(self, item_name: str) -> str:
        """Get glob pattern for source files."""
        return str(self.get_source_dir(item_name) / "*.parquet")


class ConvertTargetConfig(BaseModel):
    """Convert target path configuration (MFT_raw)."""
    model_config = ConfigDict(frozen=False)

    base_dir: Optional[str] = None
    country: str = "KR"
    subdirectory: str = "krx_crawled"
    file_pattern: str = "{ym}.parquet"
    compression: str = "lz4"
    # Permission settings
    group: Optional[str] = "qt"
    dir_mode: int = 0o2770  # rwxrws--- (owner+group full, others none, setgid)
    file_mode: int = 0o660  # rw-rw---- (owner+group read/write, others none)

    def get_base_dir(self) -> Path:
        """Get base directory with auto-detection fallback."""
        return _get_raw_base_dir(self.base_dir)

    def get_output_dir(self, item_name: str) -> Path:
        """Get output directory for a specific item."""
        return self.get_base_dir() / self.country / self.subdirectory / item_name

    def get_file_path(self, item_name: str, ym: str) -> Path:
        """Get full file path for a specific item and year-month.

        Args:
            item_name: Target name (e.g., "전종목등락률")
            ym: Year-month string in YYYY-MM format

        Returns:
            Full path to the parquet file
        """
        filename = self.file_pattern.format(ym=ym)
        return self.get_output_dir(item_name) / filename


class ConvertTargetItemConfig(BaseModel):
    """Single convert target item configuration."""
    model_config = ConfigDict(frozen=False)

    name: str
    sources: List[str]  # Source directories to merge
    output_dir: str  # Output directory name
    calc_adj_ratio: bool = False
    description: Optional[str] = None


class ConvertConfig(BaseModel):
    """Convert module configuration."""
    model_config = ConfigDict(frozen=False)

    source: ConvertSourceConfig = Field(default_factory=ConvertSourceConfig)
    target: ConvertTargetConfig = Field(default_factory=ConvertTargetConfig)
    timezone: str = "Asia/Seoul"
    targets: List[ConvertTargetItemConfig] = Field(default_factory=list)

    def get_target_by_name(self, name: str) -> Optional[ConvertTargetItemConfig]:
        """Get convert target configuration by name."""
        for target in self.targets:
            if target.name == name:
                return target
        return None


# =============================================================================
# Target Configuration
# =============================================================================

class TargetConfig(BaseModel):
    """Single crawl target configuration."""
    model_config = ConfigDict(frozen=False)

    name: str
    screen_id: str
    api_url: str
    start_date: str
    range_query: bool = False
    split_by_market: bool = False
    parametric: bool = False
    description: Optional[str] = None


# =============================================================================
# Market Configuration
# =============================================================================

class MarketInfo(BaseModel):
    """Single market configuration."""
    id: str  # "STK" or "KSQ"
    name: str  # "kospi" or "kosdaq"
    idx_ind_cd: Optional[str] = None  # For sector distribution


class MarketsConfig(BaseModel):
    """Markets configuration."""
    model_config = ConfigDict(frozen=False)

    kospi: MarketInfo = Field(
        default_factory=lambda: MarketInfo(id="STK", name="kospi", idx_ind_cd="005")
    )
    kosdaq: MarketInfo = Field(
        default_factory=lambda: MarketInfo(id="KSQ", name="kosdaq", idx_ind_cd="021")
    )

    def get_all_markets(self) -> List[MarketInfo]:
        """Get list of all markets."""
        return [self.kospi, self.kosdaq]


# =============================================================================
# Date Configuration
# =============================================================================

class HistoryDatesConfig(BaseModel):
    """Historical date boundaries."""
    saturday_trading_end: str = "1998-12-05"
    exchange_calendar_start: str = "2005-12-01"


class SpecialDatesConfig(BaseModel):
    """Special data handling dates."""
    kospi_ohlc_start: str = "1994-10-29"


class DatesConfig(BaseModel):
    """Date settings."""
    exclude: List[str] = Field(default_factory=list)
    history: HistoryDatesConfig = Field(default_factory=HistoryDatesConfig)
    special: SpecialDatesConfig = Field(default_factory=SpecialDatesConfig)


# =============================================================================
# Logging Configuration
# =============================================================================

class LoggingConfig(BaseModel):
    """Logging configuration."""
    level: str = "INFO"
    format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"


# =============================================================================
# Main Configuration
# =============================================================================

class Config(BaseModel):
    """
    Main unified configuration class.

    Contains configuration for KRX crawling and conversion.
    """
    model_config = ConfigDict(frozen=False)

    crawl: CrawlConfig = Field(default_factory=CrawlConfig)
    crawl_output: CrawlOutputConfig = Field(default_factory=CrawlOutputConfig)
    convert: ConvertConfig = Field(default_factory=ConvertConfig)
    targets: List[TargetConfig] = Field(default_factory=list)
    markets: MarketsConfig = Field(default_factory=MarketsConfig)
    dates: DatesConfig = Field(default_factory=DatesConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)
    column_names: Dict[str, str] = Field(default_factory=dict)

    # Backward compatibility property
    @property
    def output(self) -> CrawlOutputConfig:
        """Backward compatibility alias for crawl_output."""
        return self.crawl_output

    @classmethod
    def from_yaml(cls, yaml_path: Union[str, Path]) -> "Config":
        """Load configuration from YAML file."""
        yaml_path = Path(yaml_path)
        if not yaml_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {yaml_path}")

        with open(yaml_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)

        return cls.model_validate(data)

    def get_target_by_name(self, name: str) -> Optional[TargetConfig]:
        """Get crawl target configuration by name."""
        for target in self.targets:
            if target.name == name:
                return target
        return None

    def get_target_by_screen_id(self, screen_id: str) -> Optional[TargetConfig]:
        """Get crawl target configuration by screen ID."""
        for target in self.targets:
            if target.screen_id == screen_id:
                return target
        return None

    def get_convert_target_by_name(self, name: str) -> Optional[ConvertTargetItemConfig]:
        """Get convert target configuration by name."""
        return self.convert.get_target_by_name(name)


# =============================================================================
# Configuration Loading Functions
# =============================================================================

def load_config(config_path: Optional[Union[str, Path]] = None) -> Config:
    """
    Load configuration from YAML file.

    Args:
        config_path: Path to config.yaml. If None, uses default path
                    (same directory as this file).

    Returns:
        Config object with all settings.
    """
    if config_path is None:
        config_path = Path(__file__).parent / "config.yaml"

    return Config.from_yaml(config_path)


# Global config instance (lazy loaded)
_config: Optional[Config] = None


def get_config() -> Config:
    """Get or create the global config instance."""
    global _config
    if _config is None:
        _config = load_config()
    return _config


def reload_config(config_path: Optional[Union[str, Path]] = None) -> Config:
    """Reload configuration from file."""
    global _config
    _config = load_config(config_path)
    return _config
