"""MFT KRX Package - KRX data crawling utilities."""

from mft_krx.config import get_config, load_config, reload_config

__all__ = ["get_config", "load_config", "reload_config"]


def main():
    """Main entry point for python -m mft_krx."""
    import argparse
    import sys

    parser = argparse.ArgumentParser(
        description="MFT KRX - KRX data crawling and conversion utilities",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Subcommands:
  crawl     Crawl KRX market data (MFT_archive)
  convert   Convert crawled data with adj ratio (MFT_archive → MFT_raw)

Examples:
  # Show this help
  python -m mft_krx --help

  # Run crawl subcommand
  python -m mft_krx.crawl --help
  python -m mft_krx.crawl --dates 20241204

  # Run convert subcommand
  python -m mft_krx.convert --help
  python -m mft_krx.convert --target 전종목등락률

For more information on a subcommand, use:
  python -m mft_krx.crawl --help
  python -m mft_krx.convert --help
        """,
    )
    parser.add_argument(
        "--version", "-v",
        action="store_true",
        help="Show version information",
    )

    args = parser.parse_args()

    if args.version:
        print("mft_krx v1.0.0")
        return

    parser.print_help()
