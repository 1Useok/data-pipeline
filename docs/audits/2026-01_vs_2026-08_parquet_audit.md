# 2026-01 vs 2026-08+ Parquet compatibility audit

- 판정: **FAIL**
- 생성 시각(UTC): `2026-08-14T05:52:13.101513+00:00`
- 읽기 실패/손상 파일: `0`
- 2026-01: `2113` files, `60496242` rows, `63` datasets
- 2026-08+: `1410` files, `17705742` rows, `33` datasets
- 양쪽 공통 수집 종류: `33`
- `logs/` Parquet은 시장데이터 수집 종류가 아닌 운영 산출물이므로 종류 판정에서 제외
- 컬럼 스키마 불일치 수집 종류: `31`
- 엄격 포맷 불일치 수집 종류: `31`

엄격 포맷 일치는 모든 파일에서 컬럼 이름/순서, Arrow 타입, nullable, Parquet 물리 스키마,
스키마 메타데이터, writer version, compression/encoding이 하나로 동일한 경우만 통과합니다.

## 2026-01에만 존재하는 수집 종류

- `MFT_archive/KR/kis_api/investor_trading_time_call_option`
- `MFT_archive/KR/kis_api/investor_trading_time_futures`
- `MFT_archive/KR/kis_api/investor_trading_time_kosdaq150_call_option`
- `MFT_archive/KR/kis_api/investor_trading_time_kosdaq150_futures`
- `MFT_archive/KR/kis_api/investor_trading_time_kosdaq150_put_option`
- `MFT_archive/KR/kis_api/investor_trading_time_mini_call_option`
- `MFT_archive/KR/kis_api/investor_trading_time_mini_futures`
- `MFT_archive/KR/kis_api/investor_trading_time_mini_put_option`
- `MFT_archive/KR/kis_api/investor_trading_time_put_option`
- `MFT_archive/KR/krx_crawled/ETF전종목등락률`
- `MFT_archive/KR/krx_crawled/ETF전종목시세`
- `MFT_archive/KR/krx_crawled/대량매매전일`
- `MFT_archive/KR/krx_crawled/업종별분포`
- `MFT_archive/KR/krx_crawled/업종분류현황`
- `MFT_archive/KR/krx_crawled/외국인보유량`
- `MFT_archive/KR/krx_crawled/전종목등락률`
- `MFT_archive/KR/krx_crawled/전종목시세`
- `MFT_archive/KR/krx_crawled/지수구성종목`
- `MFT_archive/KR/krx_crawled/코스피지수시세추이`
- `MFT_archive/KR/krx_crawled/투자자별거래실적개별종목`
- `MFT_archive/KR/krx_crawled/파생전종목시세`
- `MFT_backtesting/KR/price/1d/future`
- `MFT_backtesting/KR/price/1d/option`
- `MFT_backtesting/KR/price/1d/stock`
- `MFT_raw/KR/price/1d/future`
- `MFT_raw/KR/price/1d/option`
- `MFT_raw/KR/price/1d/stock`
- `MFT_raw/KR/price/1h/future`
- `MFT_raw/KR/price/1h/option`
- `MFT_raw/KR/price/1h/stock`

## 2026-08+에만 존재하는 수집 종류

- 없음

## 공통 수집 종류 비교

| 수집 종류 | Jan files | Aug+ files | 컬럼 | 물리 스키마 | 메타데이터 | writer/압축 | 엄격 판정 |
|---|---:|---:|---|---|---|---|---|
| `MFT_archive/KR/kis_api/investor_trading_time_kosdaq` | 19 | 5 | PASS | PASS | PASS | PASS | PASS |
| `MFT_archive/KR/kis_api/investor_trading_time_kospi` | 19 | 5 | PASS | PASS | PASS | PASS | PASS |
| `MFT_backtesting/KR/krx_crawled/ETF전종목등락률` | 1 | 9 | FAIL | FAIL | PASS | FAIL | FAIL |
| `MFT_backtesting/KR/krx_crawled/ETF전종목시세` | 1 | 9 | FAIL | FAIL | PASS | FAIL | FAIL |
| `MFT_backtesting/KR/krx_crawled/대량매매전일` | 1 | 5 | FAIL | FAIL | PASS | FAIL | FAIL |
| `MFT_backtesting/KR/krx_crawled/업종별분포` | 1 | 9 | FAIL | FAIL | PASS | FAIL | FAIL |
| `MFT_backtesting/KR/krx_crawled/업종분류현황` | 1 | 9 | FAIL | FAIL | PASS | FAIL | FAIL |
| `MFT_backtesting/KR/krx_crawled/외국인보유량` | 1 | 9 | FAIL | FAIL | PASS | FAIL | FAIL |
| `MFT_backtesting/KR/krx_crawled/전종목등락률` | 1 | 10 | FAIL | FAIL | PASS | PASS | FAIL |
| `MFT_backtesting/KR/krx_crawled/전종목시세` | 1 | 9 | FAIL | FAIL | PASS | FAIL | FAIL |
| `MFT_backtesting/KR/krx_crawled/지수구성종목` | 163 | 1285 | FAIL | FAIL | PASS | FAIL | FAIL |
| `MFT_backtesting/KR/krx_crawled/코스피지수시세추이` | 1 | 9 | FAIL | FAIL | PASS | FAIL | FAIL |
| `MFT_backtesting/KR/krx_crawled/투자자별거래실적개별종목` | 1 | 9 | FAIL | FAIL | PASS | FAIL | FAIL |
| `MFT_backtesting/KR/krx_crawled/파생전종목시세` | 1 | 9 | FAIL | FAIL | PASS | FAIL | FAIL |
| `MFT_backtesting/KR/price/15m/future` | 1 | 1 | FAIL | FAIL | PASS | PASS | FAIL |
| `MFT_backtesting/KR/price/15m/option` | 1 | 1 | FAIL | FAIL | PASS | PASS | FAIL |
| `MFT_backtesting/KR/price/15m/stock` | 1 | 1 | FAIL | FAIL | PASS | PASS | FAIL |
| `MFT_backtesting/KR/price/1h/future` | 1 | 1 | FAIL | FAIL | PASS | PASS | FAIL |
| `MFT_backtesting/KR/price/1h/option` | 1 | 1 | FAIL | FAIL | PASS | PASS | FAIL |
| `MFT_backtesting/KR/price/1h/stock` | 1 | 1 | FAIL | FAIL | PASS | PASS | FAIL |
| `MFT_backtesting/KR/price/1m/future` | 1 | 1 | FAIL | FAIL | PASS | PASS | FAIL |
| `MFT_backtesting/KR/price/1m/option` | 1 | 1 | FAIL | FAIL | PASS | PASS | FAIL |
| `MFT_backtesting/KR/price/1m/stock` | 1 | 1 | FAIL | FAIL | PASS | PASS | FAIL |
| `MFT_backtesting/KR/price/30m/future` | 1 | 1 | FAIL | FAIL | PASS | PASS | FAIL |
| `MFT_backtesting/KR/price/30m/option` | 1 | 1 | FAIL | FAIL | PASS | PASS | FAIL |
| `MFT_backtesting/KR/price/30m/stock` | 1 | 1 | FAIL | FAIL | PASS | PASS | FAIL |
| `MFT_backtesting/KR/price/5m/future` | 1 | 1 | FAIL | FAIL | PASS | PASS | FAIL |
| `MFT_backtesting/KR/price/5m/option` | 1 | 1 | FAIL | FAIL | PASS | PASS | FAIL |
| `MFT_backtesting/KR/price/5m/stock` | 1 | 1 | FAIL | FAIL | PASS | PASS | FAIL |
| `MFT_raw/KR/price/1m/continuous_future` | 1 | 1 | FAIL | FAIL | PASS | PASS | FAIL |
| `MFT_raw/KR/price/1m/future` | 1 | 1 | FAIL | FAIL | PASS | PASS | FAIL |
| `MFT_raw/KR/price/1m/option` | 1 | 1 | FAIL | FAIL | PASS | PASS | FAIL |
| `MFT_raw/KR/price/1m/stock` | 1 | 1 | FAIL | FAIL | PASS | PASS | FAIL |

## 불일치 상세

### `MFT_backtesting/KR/krx_crawled/ETF전종목등락률`

- Jan logical variants: `1`
- Aug+ logical variants: `2`
- Jan physical variants: `1`
- Aug+ physical variants: `2`
- Jan metadata variants: `1`
- Aug+ metadata variants: `1`
- Jan writer variants: `1`
- Aug+ writer variants: `1`
- Jan-only columns: `['base_price', 'change', 'change_rate', 'datetime', 'end_close', 'isin', 'korean_name', 'ticker', 'trading_value', 'volume']`
- Aug-only columns: `['date', 'sec_code', '거래대금', '거래량', '대비', '등락률', '시작일 기준가', '종료일 종가', '종목명', '종목코드']`
- Type/nullability changes: `[]`
- Column order equal: `False`
- Representative Jan writer: `{'format_version': '1.0', 'created_by': 'Polars', 'compressions': ['ZSTD'], 'encodings': ['PLAIN', 'RLE', 'RLE_DICTIONARY']}`
- Representative Aug writer: `{'format_version': '1.0', 'created_by': 'Polars', 'compressions': ['LZ4'], 'encodings': ['PLAIN', 'RLE', 'RLE_DICTIONARY']}`

### `MFT_backtesting/KR/krx_crawled/ETF전종목시세`

- Jan logical variants: `1`
- Aug+ logical variants: `2`
- Jan physical variants: `1`
- Aug+ physical variants: `2`
- Jan metadata variants: `1`
- Aug+ metadata variants: `1`
- Jan writer variants: `1`
- Aug+ writer variants: `1`
- Jan-only columns: `['change', 'change_rate', 'change_type', 'close', 'datetime', 'high', 'isin', 'korean_name', 'low', 'market_cap', 'nav', 'net_asset_value', 'open', 'security_group_id', 'shares_outstanding', 'ticker', 'trading_value', 'underlying_index_change', 'underlying_index_change_rate', 'underlying_index_change_type', 'underlying_index_close', 'underlying_index_name', 'volume']`
- Aug-only columns: `['NAV', 'date', 'sec_code', '거래대금', '거래량', '고가', '기초지수대비', '기초지수등락구분', '기초지수등락률', '기초지수명', '기초지수종가', '대비', '등락구분', '등락률', '상장주식수', '순자산총액', '시가', '시가총액', '저가', '종가', '종목명', '종목코드', '증권그룹ID']`
- Type/nullability changes: `[]`
- Column order equal: `False`
- Representative Jan writer: `{'format_version': '1.0', 'created_by': 'Polars', 'compressions': ['ZSTD'], 'encodings': ['PLAIN', 'RLE', 'RLE_DICTIONARY']}`
- Representative Aug writer: `{'format_version': '1.0', 'created_by': 'Polars', 'compressions': ['LZ4'], 'encodings': ['PLAIN', 'RLE', 'RLE_DICTIONARY']}`

### `MFT_backtesting/KR/krx_crawled/대량매매전일`

- Jan logical variants: `1`
- Aug+ logical variants: `1`
- Jan physical variants: `1`
- Aug+ physical variants: `1`
- Jan metadata variants: `1`
- Aug+ metadata variants: `1`
- Jan writer variants: `1`
- Aug+ writer variants: `1`
- Jan-only columns: `['block_trade_ratio', 'block_trade_volume', 'change', 'change_rate', 'close', 'datetime', 'korean_name', 'ticker', 'volume']`
- Aug-only columns: `['date', '거래량', '대량매매비율', '대량매매수량', '대비', '등락률', '종가', '종목명', '종목코드']`
- Type/nullability changes: `[]`
- Column order equal: `False`
- Representative Jan writer: `{'format_version': '1.0', 'created_by': 'Polars', 'compressions': ['ZSTD'], 'encodings': ['PLAIN', 'RLE', 'RLE_DICTIONARY']}`
- Representative Aug writer: `{'format_version': '1.0', 'created_by': 'Polars', 'compressions': ['LZ4'], 'encodings': ['PLAIN', 'RLE', 'RLE_DICTIONARY']}`

### `MFT_backtesting/KR/krx_crawled/업종별분포`

- Jan logical variants: `1`
- Aug+ logical variants: `2`
- Jan physical variants: `1`
- Aug+ physical variants: `2`
- Jan metadata variants: `1`
- Aug+ metadata variants: `1`
- Jan writer variants: `1`
- Aug+ writer variants: `1`
- Jan-only columns: `['company_count', 'datetime', 'market_cap_amount', 'market_cap_weight', 'sector_name', 'shares_outstanding', 'stock_count', 'trading_value_amount', 'trading_value_weight', 'volume_quantity', 'volume_weight']`
- Aug-only columns: `['date', '거래대금_금액', '거래대금_비중', '거래량_비중', '거래량_수량', '상장주식수', '시가총액_금액', '시가총액_비중', '업종명', '종목수', '회사수']`
- Type/nullability changes: `[]`
- Column order equal: `False`
- Representative Jan writer: `{'format_version': '1.0', 'created_by': 'Polars', 'compressions': ['ZSTD'], 'encodings': ['PLAIN', 'RLE', 'RLE_DICTIONARY']}`
- Representative Aug writer: `{'format_version': '1.0', 'created_by': 'Polars', 'compressions': ['LZ4'], 'encodings': ['PLAIN', 'RLE', 'RLE_DICTIONARY']}`

### `MFT_backtesting/KR/krx_crawled/업종분류현황`

- Jan logical variants: `1`
- Aug+ logical variants: `2`
- Jan physical variants: `1`
- Aug+ physical variants: `2`
- Jan metadata variants: `1`
- Aug+ metadata variants: `1`
- Jan writer variants: `1`
- Aug+ writer variants: `1`
- Jan-only columns: `['change', 'change_rate', 'close', 'datetime', 'isin', 'korean_name', 'market_cap', 'market_type', 'sector_name', 'ticker']`
- Aug-only columns: `['date', 'sec_code', '대비', '등락률', '시가총액', '시장구분', '업종명', '종가', '종목명', '종목코드']`
- Type/nullability changes: `[]`
- Column order equal: `False`
- Representative Jan writer: `{'format_version': '1.0', 'created_by': 'Polars', 'compressions': ['ZSTD'], 'encodings': ['PLAIN', 'RLE', 'RLE_DICTIONARY']}`
- Representative Aug writer: `{'format_version': '1.0', 'created_by': 'Polars', 'compressions': ['LZ4'], 'encodings': ['PLAIN', 'RLE', 'RLE_DICTIONARY']}`

### `MFT_backtesting/KR/krx_crawled/외국인보유량`

- Jan logical variants: `1`
- Aug+ logical variants: `2`
- Jan physical variants: `1`
- Aug+ physical variants: `2`
- Jan metadata variants: `1`
- Aug+ metadata variants: `1`
- Jan writer variants: `1`
- Aug+ writer variants: `1`
- Jan-only columns: `['change', 'change_rate', 'close', 'datetime', 'foreign_holding', 'foreign_limit', 'foreign_limit_exhaustion_rate', 'foreign_ownership_rate', 'isin', 'korean_name', 'shares_outstanding', 'ticker']`
- Aug-only columns: `['date', 'sec_code', '대비', '등락률', '상장주식수', '외국인 보유수량', '외국인 지분율', '외국인 한도소진율', '외국인 한도수량', '종가', '종목명', '종목코드']`
- Type/nullability changes: `[]`
- Column order equal: `False`
- Representative Jan writer: `{'format_version': '1.0', 'created_by': 'Polars', 'compressions': ['ZSTD'], 'encodings': ['PLAIN', 'RLE', 'RLE_DICTIONARY']}`
- Representative Aug writer: `{'format_version': '1.0', 'created_by': 'Polars', 'compressions': ['LZ4'], 'encodings': ['PLAIN', 'RLE', 'RLE_DICTIONARY']}`

### `MFT_backtesting/KR/krx_crawled/전종목등락률`

- Jan logical variants: `1`
- Aug+ logical variants: `3`
- Jan physical variants: `1`
- Aug+ physical variants: `3`
- Jan metadata variants: `1`
- Aug+ metadata variants: `1`
- Jan writer variants: `1`
- Aug+ writer variants: `1`
- Jan-only columns: `['adj_ratio', 'base_price', 'change', 'change_rate', 'cum_adj_ratio', 'datetime', 'end_close', 'isin', 'korean_name', 'ticker', 'trading_value', 'volume']`
- Aug-only columns: `['date', 'sec_code', '거래대금', '거래량', '대비', '등락률', '시작일 기준가', '종료일 종가', '종목명', '종목코드']`
- Type/nullability changes: `[]`
- Column order equal: `False`
- Representative Jan writer: `{'format_version': '1.0', 'created_by': 'Polars', 'compressions': ['LZ4'], 'encodings': ['PLAIN', 'RLE', 'RLE_DICTIONARY']}`
- Representative Aug writer: `{'format_version': '1.0', 'created_by': 'Polars', 'compressions': ['LZ4'], 'encodings': ['PLAIN', 'RLE', 'RLE_DICTIONARY']}`

### `MFT_backtesting/KR/krx_crawled/전종목시세`

- Jan logical variants: `1`
- Aug+ logical variants: `2`
- Jan physical variants: `1`
- Aug+ physical variants: `2`
- Jan metadata variants: `1`
- Aug+ metadata variants: `1`
- Jan writer variants: `1`
- Aug+ writer variants: `1`
- Jan-only columns: `['change', 'change_rate', 'close', 'datetime', 'division', 'high', 'isin', 'korean_name', 'low', 'market_cap', 'market_type', 'open', 'shares_outstanding', 'ticker', 'trading_value', 'volume']`
- Aug-only columns: `['date', 'sec_code', '거래대금', '거래량', '고가', '대비', '등락률', '상장주식수', '소속부', '시가', '시가총액', '시장구분', '저가', '종가', '종목명', '종목코드']`
- Type/nullability changes: `[]`
- Column order equal: `False`
- Representative Jan writer: `{'format_version': '1.0', 'created_by': 'Polars', 'compressions': ['ZSTD'], 'encodings': ['PLAIN', 'RLE', 'RLE_DICTIONARY']}`
- Representative Aug writer: `{'format_version': '1.0', 'created_by': 'Polars', 'compressions': ['LZ4'], 'encodings': ['PLAIN', 'RLE', 'RLE_DICTIONARY']}`

### `MFT_backtesting/KR/krx_crawled/지수구성종목`

- Jan logical variants: `1`
- Aug+ logical variants: `2`
- Jan physical variants: `1`
- Aug+ physical variants: `2`
- Jan metadata variants: `1`
- Aug+ metadata variants: `1`
- Jan writer variants: `1`
- Aug+ writer variants: `1`
- Jan-only columns: `['change', 'change_rate', 'close', 'datetime', 'isin', 'korean_name', 'market_cap', 'ticker']`
- Aug-only columns: `['date', 'sec_code', '대비', '등락률', '상장시가총액', '종가', '종목명', '종목코드']`
- Type/nullability changes: `[]`
- Column order equal: `False`
- Representative Jan writer: `{'format_version': '1.0', 'created_by': 'Polars', 'compressions': ['ZSTD'], 'encodings': ['PLAIN', 'RLE', 'RLE_DICTIONARY']}`
- Representative Aug writer: `{'format_version': '1.0', 'created_by': 'Polars', 'compressions': ['LZ4'], 'encodings': ['PLAIN', 'RLE', 'RLE_DICTIONARY']}`

### `MFT_backtesting/KR/krx_crawled/코스피지수시세추이`

- Jan logical variants: `1`
- Aug+ logical variants: `1`
- Jan physical variants: `1`
- Aug+ physical variants: `1`
- Jan metadata variants: `1`
- Aug+ metadata variants: `1`
- Jan writer variants: `1`
- Aug+ writer variants: `1`
- Jan-only columns: `['change', 'change_rate', 'close', 'datetime', 'high', 'low', 'market_cap', 'open', 'trading_value', 'volume']`
- Aug-only columns: `['date', '거래대금', '거래량', '고가', '대비', '등락률', '상장시가총액', '시가', '저가', '종가']`
- Type/nullability changes: `[]`
- Column order equal: `False`
- Representative Jan writer: `{'format_version': '1.0', 'created_by': 'Polars', 'compressions': ['ZSTD'], 'encodings': ['PLAIN', 'RLE', 'RLE_DICTIONARY']}`
- Representative Aug writer: `{'format_version': '1.0', 'created_by': 'Polars', 'compressions': ['LZ4'], 'encodings': ['PLAIN', 'RLE', 'RLE_DICTIONARY']}`

### `MFT_backtesting/KR/krx_crawled/투자자별거래실적개별종목`

- Jan logical variants: `1`
- Aug+ logical variants: `2`
- Jan physical variants: `1`
- Aug+ physical variants: `2`
- Jan metadata variants: `1`
- Aug+ metadata variants: `1`
- Jan writer variants: `1`
- Aug+ writer variants: `1`
- Jan-only columns: `['bank', 'datetime', 'financial_investment', 'foreigner', 'individual', 'insurance', 'investment_trust', 'isin', 'other_corporation', 'other_financial', 'other_foreigner', 'pension_fund', 'private_equity', 'ticker', 'total']`
- Aug-only columns: `['date', 'sec_code', '개인', '금융투자', '기타금융', '기타법인', '기타외국인', '보험', '사모', '연기금', '외국인', '은행', '전체', '종목명', '종목코드', '투신']`
- Type/nullability changes: `[]`
- Column order equal: `False`
- Representative Jan writer: `{'format_version': '1.0', 'created_by': 'Polars', 'compressions': ['ZSTD'], 'encodings': ['PLAIN', 'RLE', 'RLE_DICTIONARY']}`
- Representative Aug writer: `{'format_version': '1.0', 'created_by': 'Polars', 'compressions': ['LZ4'], 'encodings': ['PLAIN', 'RLE', 'RLE_DICTIONARY']}`

### `MFT_backtesting/KR/krx_crawled/파생전종목시세`

- Jan logical variants: `1`
- Aug+ logical variants: `2`
- Jan physical variants: `1`
- Aug+ physical variants: `2`
- Jan metadata variants: `1`
- Aug+ metadata variants: `1`
- Jan writer variants: `1`
- Aug+ writer variants: `1`
- Jan-only columns: `['change', 'change_type', 'close', 'datetime', 'high', 'isin', 'korean_name', 'low', 'open', 'open_interest', 'security_group_id', 'settlement_price', 'spot_price', 'ticker', 'trading_value', 'volume']`
- Aug-only columns: `['date', 'sec_code', '거래대금', '거래량', '고가', '대비', '등락구분', '미결제약정', '시가', '저가', '정산가', '종가', '종목명', '종목코드', '증권그룹ID', '현물가']`
- Type/nullability changes: `[]`
- Column order equal: `False`
- Representative Jan writer: `{'format_version': '1.0', 'created_by': 'Polars', 'compressions': ['ZSTD'], 'encodings': ['PLAIN', 'RLE', 'RLE_DICTIONARY']}`
- Representative Aug writer: `{'format_version': '1.0', 'created_by': 'Polars', 'compressions': ['LZ4'], 'encodings': ['PLAIN', 'RLE', 'RLE_DICTIONARY']}`

### `MFT_backtesting/KR/price/15m/future`

- Jan logical variants: `1`
- Aug+ logical variants: `1`
- Jan physical variants: `1`
- Aug+ physical variants: `1`
- Jan metadata variants: `1`
- Aug+ metadata variants: `1`
- Jan writer variants: `1`
- Aug+ writer variants: `1`
- Jan-only columns: `['cum_value']`
- Aug-only columns: `[]`
- Type/nullability changes: `[{'name': 'datetime', 'january_type': 'timestamp[ns, tz=Asia/Seoul]', 'august_type': 'timestamp[ns]', 'january_nullable': True, 'august_nullable': True}, {'name': 'volume', 'january_type': 'double', 'august_type': 'int64', 'january_nullable': True, 'august_nullable': True}]`
- Column order equal: `False`
- Representative Jan writer: `{'format_version': '1.0', 'created_by': 'Polars', 'compressions': ['LZ4'], 'encodings': ['PLAIN', 'RLE', 'RLE_DICTIONARY']}`
- Representative Aug writer: `{'format_version': '1.0', 'created_by': 'Polars', 'compressions': ['LZ4'], 'encodings': ['PLAIN', 'RLE', 'RLE_DICTIONARY']}`

### `MFT_backtesting/KR/price/15m/option`

- Jan logical variants: `1`
- Aug+ logical variants: `1`
- Jan physical variants: `1`
- Aug+ physical variants: `1`
- Jan metadata variants: `1`
- Aug+ metadata variants: `1`
- Jan writer variants: `1`
- Aug+ writer variants: `1`
- Jan-only columns: `['cum_value']`
- Aug-only columns: `[]`
- Type/nullability changes: `[{'name': 'datetime', 'january_type': 'timestamp[ns, tz=Asia/Seoul]', 'august_type': 'timestamp[ns]', 'january_nullable': True, 'august_nullable': True}, {'name': 'volume', 'january_type': 'double', 'august_type': 'int64', 'january_nullable': True, 'august_nullable': True}]`
- Column order equal: `False`
- Representative Jan writer: `{'format_version': '1.0', 'created_by': 'Polars', 'compressions': ['LZ4'], 'encodings': ['PLAIN', 'RLE', 'RLE_DICTIONARY']}`
- Representative Aug writer: `{'format_version': '1.0', 'created_by': 'Polars', 'compressions': ['LZ4'], 'encodings': ['PLAIN', 'RLE', 'RLE_DICTIONARY']}`

### `MFT_backtesting/KR/price/15m/stock`

- Jan logical variants: `1`
- Aug+ logical variants: `1`
- Jan physical variants: `1`
- Aug+ physical variants: `1`
- Jan metadata variants: `1`
- Aug+ metadata variants: `1`
- Jan writer variants: `1`
- Aug+ writer variants: `1`
- Jan-only columns: `['adj_first_ask', 'adj_first_bid', 'adj_last_ask', 'adj_last_bid', 'cum_adj_ratio', 'cum_value', 'first_ask', 'first_bid', 'last_ask', 'last_bid', 'ticker']`
- Aug-only columns: `[]`
- Type/nullability changes: `[{'name': 'datetime', 'january_type': 'timestamp[ns, tz=Asia/Seoul]', 'august_type': 'timestamp[ns]', 'january_nullable': True, 'august_nullable': True}, {'name': 'volume', 'january_type': 'double', 'august_type': 'int64', 'january_nullable': True, 'august_nullable': True}]`
- Column order equal: `False`
- Representative Jan writer: `{'format_version': '1.0', 'created_by': 'Polars', 'compressions': ['LZ4'], 'encodings': ['PLAIN', 'RLE', 'RLE_DICTIONARY']}`
- Representative Aug writer: `{'format_version': '1.0', 'created_by': 'Polars', 'compressions': ['LZ4'], 'encodings': ['PLAIN', 'RLE', 'RLE_DICTIONARY']}`

### `MFT_backtesting/KR/price/1h/future`

- Jan logical variants: `1`
- Aug+ logical variants: `1`
- Jan physical variants: `1`
- Aug+ physical variants: `1`
- Jan metadata variants: `1`
- Aug+ metadata variants: `1`
- Jan writer variants: `1`
- Aug+ writer variants: `1`
- Jan-only columns: `['cum_value']`
- Aug-only columns: `[]`
- Type/nullability changes: `[{'name': 'datetime', 'january_type': 'timestamp[ns, tz=Asia/Seoul]', 'august_type': 'timestamp[ns]', 'january_nullable': True, 'august_nullable': True}, {'name': 'volume', 'january_type': 'double', 'august_type': 'int64', 'january_nullable': True, 'august_nullable': True}]`
- Column order equal: `False`
- Representative Jan writer: `{'format_version': '1.0', 'created_by': 'Polars', 'compressions': ['LZ4'], 'encodings': ['PLAIN', 'RLE', 'RLE_DICTIONARY']}`
- Representative Aug writer: `{'format_version': '1.0', 'created_by': 'Polars', 'compressions': ['LZ4'], 'encodings': ['PLAIN', 'RLE', 'RLE_DICTIONARY']}`

### `MFT_backtesting/KR/price/1h/option`

- Jan logical variants: `1`
- Aug+ logical variants: `1`
- Jan physical variants: `1`
- Aug+ physical variants: `1`
- Jan metadata variants: `1`
- Aug+ metadata variants: `1`
- Jan writer variants: `1`
- Aug+ writer variants: `1`
- Jan-only columns: `['cum_value']`
- Aug-only columns: `[]`
- Type/nullability changes: `[{'name': 'datetime', 'january_type': 'timestamp[ns, tz=Asia/Seoul]', 'august_type': 'timestamp[ns]', 'january_nullable': True, 'august_nullable': True}, {'name': 'volume', 'january_type': 'double', 'august_type': 'int64', 'january_nullable': True, 'august_nullable': True}]`
- Column order equal: `False`
- Representative Jan writer: `{'format_version': '1.0', 'created_by': 'Polars', 'compressions': ['LZ4'], 'encodings': ['PLAIN', 'RLE', 'RLE_DICTIONARY']}`
- Representative Aug writer: `{'format_version': '1.0', 'created_by': 'Polars', 'compressions': ['LZ4'], 'encodings': ['PLAIN', 'RLE', 'RLE_DICTIONARY']}`

### `MFT_backtesting/KR/price/1h/stock`

- Jan logical variants: `1`
- Aug+ logical variants: `1`
- Jan physical variants: `1`
- Aug+ physical variants: `1`
- Jan metadata variants: `1`
- Aug+ metadata variants: `1`
- Jan writer variants: `1`
- Aug+ writer variants: `1`
- Jan-only columns: `['adj_first_ask', 'adj_first_bid', 'adj_last_ask', 'adj_last_bid', 'cum_adj_ratio', 'cum_value', 'first_ask', 'first_bid', 'last_ask', 'last_bid', 'ticker']`
- Aug-only columns: `[]`
- Type/nullability changes: `[{'name': 'datetime', 'january_type': 'timestamp[ns, tz=Asia/Seoul]', 'august_type': 'timestamp[ns]', 'january_nullable': True, 'august_nullable': True}, {'name': 'volume', 'january_type': 'double', 'august_type': 'int64', 'january_nullable': True, 'august_nullable': True}]`
- Column order equal: `False`
- Representative Jan writer: `{'format_version': '1.0', 'created_by': 'Polars', 'compressions': ['LZ4'], 'encodings': ['PLAIN', 'RLE', 'RLE_DICTIONARY']}`
- Representative Aug writer: `{'format_version': '1.0', 'created_by': 'Polars', 'compressions': ['LZ4'], 'encodings': ['PLAIN', 'RLE', 'RLE_DICTIONARY']}`

### `MFT_backtesting/KR/price/1m/future`

- Jan logical variants: `1`
- Aug+ logical variants: `1`
- Jan physical variants: `1`
- Aug+ physical variants: `1`
- Jan metadata variants: `1`
- Aug+ metadata variants: `1`
- Jan writer variants: `1`
- Aug+ writer variants: `1`
- Jan-only columns: `['cum_value']`
- Aug-only columns: `[]`
- Type/nullability changes: `[{'name': 'datetime', 'january_type': 'timestamp[ns, tz=Asia/Seoul]', 'august_type': 'timestamp[ns]', 'january_nullable': True, 'august_nullable': True}, {'name': 'volume', 'january_type': 'double', 'august_type': 'int64', 'january_nullable': True, 'august_nullable': True}]`
- Column order equal: `False`
- Representative Jan writer: `{'format_version': '1.0', 'created_by': 'Polars', 'compressions': ['LZ4'], 'encodings': ['PLAIN', 'RLE', 'RLE_DICTIONARY']}`
- Representative Aug writer: `{'format_version': '1.0', 'created_by': 'Polars', 'compressions': ['LZ4'], 'encodings': ['PLAIN', 'RLE', 'RLE_DICTIONARY']}`

### `MFT_backtesting/KR/price/1m/option`

- Jan logical variants: `1`
- Aug+ logical variants: `1`
- Jan physical variants: `1`
- Aug+ physical variants: `1`
- Jan metadata variants: `1`
- Aug+ metadata variants: `1`
- Jan writer variants: `1`
- Aug+ writer variants: `1`
- Jan-only columns: `['cum_value']`
- Aug-only columns: `[]`
- Type/nullability changes: `[{'name': 'datetime', 'january_type': 'timestamp[ns, tz=Asia/Seoul]', 'august_type': 'timestamp[ns]', 'january_nullable': True, 'august_nullable': True}, {'name': 'volume', 'january_type': 'double', 'august_type': 'int64', 'january_nullable': True, 'august_nullable': True}]`
- Column order equal: `False`
- Representative Jan writer: `{'format_version': '1.0', 'created_by': 'Polars', 'compressions': ['LZ4'], 'encodings': ['PLAIN', 'RLE', 'RLE_DICTIONARY']}`
- Representative Aug writer: `{'format_version': '1.0', 'created_by': 'Polars', 'compressions': ['LZ4'], 'encodings': ['PLAIN', 'RLE', 'RLE_DICTIONARY']}`

### `MFT_backtesting/KR/price/1m/stock`

- Jan logical variants: `1`
- Aug+ logical variants: `1`
- Jan physical variants: `1`
- Aug+ physical variants: `1`
- Jan metadata variants: `1`
- Aug+ metadata variants: `1`
- Jan writer variants: `1`
- Aug+ writer variants: `1`
- Jan-only columns: `['adj_first_ask', 'adj_first_bid', 'adj_last_ask', 'adj_last_bid', 'cum_value', 'first_ask', 'first_bid', 'last_ask', 'last_bid']`
- Aug-only columns: `[]`
- Type/nullability changes: `[{'name': 'datetime', 'january_type': 'timestamp[ns, tz=Asia/Seoul]', 'august_type': 'timestamp[ns]', 'january_nullable': True, 'august_nullable': True}, {'name': 'volume', 'january_type': 'double', 'august_type': 'int64', 'january_nullable': True, 'august_nullable': True}]`
- Column order equal: `False`
- Representative Jan writer: `{'format_version': '1.0', 'created_by': 'Polars', 'compressions': ['LZ4'], 'encodings': ['PLAIN', 'RLE', 'RLE_DICTIONARY']}`
- Representative Aug writer: `{'format_version': '1.0', 'created_by': 'Polars', 'compressions': ['LZ4'], 'encodings': ['PLAIN', 'RLE', 'RLE_DICTIONARY']}`

### `MFT_backtesting/KR/price/30m/future`

- Jan logical variants: `1`
- Aug+ logical variants: `1`
- Jan physical variants: `1`
- Aug+ physical variants: `1`
- Jan metadata variants: `1`
- Aug+ metadata variants: `1`
- Jan writer variants: `1`
- Aug+ writer variants: `1`
- Jan-only columns: `['cum_value']`
- Aug-only columns: `[]`
- Type/nullability changes: `[{'name': 'datetime', 'january_type': 'timestamp[ns, tz=Asia/Seoul]', 'august_type': 'timestamp[ns]', 'january_nullable': True, 'august_nullable': True}, {'name': 'volume', 'january_type': 'double', 'august_type': 'int64', 'january_nullable': True, 'august_nullable': True}]`
- Column order equal: `False`
- Representative Jan writer: `{'format_version': '1.0', 'created_by': 'Polars', 'compressions': ['LZ4'], 'encodings': ['PLAIN', 'RLE', 'RLE_DICTIONARY']}`
- Representative Aug writer: `{'format_version': '1.0', 'created_by': 'Polars', 'compressions': ['LZ4'], 'encodings': ['PLAIN', 'RLE', 'RLE_DICTIONARY']}`

### `MFT_backtesting/KR/price/30m/option`

- Jan logical variants: `1`
- Aug+ logical variants: `1`
- Jan physical variants: `1`
- Aug+ physical variants: `1`
- Jan metadata variants: `1`
- Aug+ metadata variants: `1`
- Jan writer variants: `1`
- Aug+ writer variants: `1`
- Jan-only columns: `['cum_value']`
- Aug-only columns: `[]`
- Type/nullability changes: `[{'name': 'datetime', 'january_type': 'timestamp[ns, tz=Asia/Seoul]', 'august_type': 'timestamp[ns]', 'january_nullable': True, 'august_nullable': True}, {'name': 'volume', 'january_type': 'double', 'august_type': 'int64', 'january_nullable': True, 'august_nullable': True}]`
- Column order equal: `False`
- Representative Jan writer: `{'format_version': '1.0', 'created_by': 'Polars', 'compressions': ['LZ4'], 'encodings': ['PLAIN', 'RLE', 'RLE_DICTIONARY']}`
- Representative Aug writer: `{'format_version': '1.0', 'created_by': 'Polars', 'compressions': ['LZ4'], 'encodings': ['PLAIN', 'RLE', 'RLE_DICTIONARY']}`

### `MFT_backtesting/KR/price/30m/stock`

- Jan logical variants: `1`
- Aug+ logical variants: `1`
- Jan physical variants: `1`
- Aug+ physical variants: `1`
- Jan metadata variants: `1`
- Aug+ metadata variants: `1`
- Jan writer variants: `1`
- Aug+ writer variants: `1`
- Jan-only columns: `['adj_first_ask', 'adj_first_bid', 'adj_last_ask', 'adj_last_bid', 'cum_adj_ratio', 'cum_value', 'first_ask', 'first_bid', 'last_ask', 'last_bid', 'ticker']`
- Aug-only columns: `[]`
- Type/nullability changes: `[{'name': 'datetime', 'january_type': 'timestamp[ns, tz=Asia/Seoul]', 'august_type': 'timestamp[ns]', 'january_nullable': True, 'august_nullable': True}, {'name': 'volume', 'january_type': 'double', 'august_type': 'int64', 'january_nullable': True, 'august_nullable': True}]`
- Column order equal: `False`
- Representative Jan writer: `{'format_version': '1.0', 'created_by': 'Polars', 'compressions': ['LZ4'], 'encodings': ['PLAIN', 'RLE', 'RLE_DICTIONARY']}`
- Representative Aug writer: `{'format_version': '1.0', 'created_by': 'Polars', 'compressions': ['LZ4'], 'encodings': ['PLAIN', 'RLE', 'RLE_DICTIONARY']}`

### `MFT_backtesting/KR/price/5m/future`

- Jan logical variants: `1`
- Aug+ logical variants: `1`
- Jan physical variants: `1`
- Aug+ physical variants: `1`
- Jan metadata variants: `1`
- Aug+ metadata variants: `1`
- Jan writer variants: `1`
- Aug+ writer variants: `1`
- Jan-only columns: `['cum_value']`
- Aug-only columns: `[]`
- Type/nullability changes: `[{'name': 'datetime', 'january_type': 'timestamp[ns, tz=Asia/Seoul]', 'august_type': 'timestamp[ns]', 'january_nullable': True, 'august_nullable': True}, {'name': 'volume', 'january_type': 'double', 'august_type': 'int64', 'january_nullable': True, 'august_nullable': True}]`
- Column order equal: `False`
- Representative Jan writer: `{'format_version': '1.0', 'created_by': 'Polars', 'compressions': ['LZ4'], 'encodings': ['PLAIN', 'RLE', 'RLE_DICTIONARY']}`
- Representative Aug writer: `{'format_version': '1.0', 'created_by': 'Polars', 'compressions': ['LZ4'], 'encodings': ['PLAIN', 'RLE', 'RLE_DICTIONARY']}`

### `MFT_backtesting/KR/price/5m/option`

- Jan logical variants: `1`
- Aug+ logical variants: `1`
- Jan physical variants: `1`
- Aug+ physical variants: `1`
- Jan metadata variants: `1`
- Aug+ metadata variants: `1`
- Jan writer variants: `1`
- Aug+ writer variants: `1`
- Jan-only columns: `['cum_value']`
- Aug-only columns: `[]`
- Type/nullability changes: `[{'name': 'datetime', 'january_type': 'timestamp[ns, tz=Asia/Seoul]', 'august_type': 'timestamp[ns]', 'january_nullable': True, 'august_nullable': True}, {'name': 'volume', 'january_type': 'double', 'august_type': 'int64', 'january_nullable': True, 'august_nullable': True}]`
- Column order equal: `False`
- Representative Jan writer: `{'format_version': '1.0', 'created_by': 'Polars', 'compressions': ['LZ4'], 'encodings': ['PLAIN', 'RLE', 'RLE_DICTIONARY']}`
- Representative Aug writer: `{'format_version': '1.0', 'created_by': 'Polars', 'compressions': ['LZ4'], 'encodings': ['PLAIN', 'RLE', 'RLE_DICTIONARY']}`

### `MFT_backtesting/KR/price/5m/stock`

- Jan logical variants: `1`
- Aug+ logical variants: `1`
- Jan physical variants: `1`
- Aug+ physical variants: `1`
- Jan metadata variants: `1`
- Aug+ metadata variants: `1`
- Jan writer variants: `1`
- Aug+ writer variants: `1`
- Jan-only columns: `['adj_first_ask', 'adj_first_bid', 'adj_last_ask', 'adj_last_bid', 'cum_adj_ratio', 'cum_value', 'first_ask', 'first_bid', 'last_ask', 'last_bid', 'ticker']`
- Aug-only columns: `[]`
- Type/nullability changes: `[{'name': 'datetime', 'january_type': 'timestamp[ns, tz=Asia/Seoul]', 'august_type': 'timestamp[ns]', 'january_nullable': True, 'august_nullable': True}, {'name': 'volume', 'january_type': 'double', 'august_type': 'int64', 'january_nullable': True, 'august_nullable': True}]`
- Column order equal: `False`
- Representative Jan writer: `{'format_version': '1.0', 'created_by': 'Polars', 'compressions': ['LZ4'], 'encodings': ['PLAIN', 'RLE', 'RLE_DICTIONARY']}`
- Representative Aug writer: `{'format_version': '1.0', 'created_by': 'Polars', 'compressions': ['LZ4'], 'encodings': ['PLAIN', 'RLE', 'RLE_DICTIONARY']}`

### `MFT_raw/KR/price/1m/continuous_future`

- Jan logical variants: `1`
- Aug+ logical variants: `1`
- Jan physical variants: `1`
- Aug+ physical variants: `1`
- Jan metadata variants: `1`
- Aug+ metadata variants: `1`
- Jan writer variants: `1`
- Aug+ writer variants: `1`
- Jan-only columns: `['cum_value', 'rollover_spread']`
- Aug-only columns: `[]`
- Type/nullability changes: `[{'name': 'trade_time', 'january_type': 'timestamp[us, tz=Asia/Seoul]', 'august_type': 'timestamp[ns]', 'january_nullable': True, 'august_nullable': True}, {'name': 'trade_volume', 'january_type': 'double', 'august_type': 'int64', 'january_nullable': True, 'august_nullable': True}]`
- Column order equal: `False`
- Representative Jan writer: `{'format_version': '1.0', 'created_by': 'Polars', 'compressions': ['LZ4'], 'encodings': ['PLAIN', 'RLE', 'RLE_DICTIONARY']}`
- Representative Aug writer: `{'format_version': '1.0', 'created_by': 'Polars', 'compressions': ['LZ4'], 'encodings': ['PLAIN', 'RLE', 'RLE_DICTIONARY']}`

### `MFT_raw/KR/price/1m/future`

- Jan logical variants: `1`
- Aug+ logical variants: `1`
- Jan physical variants: `1`
- Aug+ physical variants: `1`
- Jan metadata variants: `1`
- Aug+ metadata variants: `1`
- Jan writer variants: `1`
- Aug+ writer variants: `1`
- Jan-only columns: `['cum_value']`
- Aug-only columns: `[]`
- Type/nullability changes: `[{'name': 'trade_time', 'january_type': 'timestamp[us, tz=Asia/Seoul]', 'august_type': 'timestamp[ns]', 'january_nullable': True, 'august_nullable': True}, {'name': 'trade_volume', 'january_type': 'double', 'august_type': 'int64', 'january_nullable': True, 'august_nullable': True}]`
- Column order equal: `False`
- Representative Jan writer: `{'format_version': '1.0', 'created_by': 'Polars', 'compressions': ['LZ4'], 'encodings': ['PLAIN', 'RLE', 'RLE_DICTIONARY']}`
- Representative Aug writer: `{'format_version': '1.0', 'created_by': 'Polars', 'compressions': ['LZ4'], 'encodings': ['PLAIN', 'RLE', 'RLE_DICTIONARY']}`

### `MFT_raw/KR/price/1m/option`

- Jan logical variants: `1`
- Aug+ logical variants: `1`
- Jan physical variants: `1`
- Aug+ physical variants: `1`
- Jan metadata variants: `1`
- Aug+ metadata variants: `1`
- Jan writer variants: `1`
- Aug+ writer variants: `1`
- Jan-only columns: `['cum_value']`
- Aug-only columns: `[]`
- Type/nullability changes: `[{'name': 'trade_time', 'january_type': 'timestamp[us, tz=Asia/Seoul]', 'august_type': 'timestamp[ns]', 'january_nullable': True, 'august_nullable': True}, {'name': 'trade_volume', 'january_type': 'double', 'august_type': 'int64', 'january_nullable': True, 'august_nullable': True}]`
- Column order equal: `False`
- Representative Jan writer: `{'format_version': '1.0', 'created_by': 'Polars', 'compressions': ['LZ4'], 'encodings': ['PLAIN', 'RLE', 'RLE_DICTIONARY']}`
- Representative Aug writer: `{'format_version': '1.0', 'created_by': 'Polars', 'compressions': ['LZ4'], 'encodings': ['PLAIN', 'RLE', 'RLE_DICTIONARY']}`

### `MFT_raw/KR/price/1m/stock`

- Jan logical variants: `1`
- Aug+ logical variants: `1`
- Jan physical variants: `1`
- Aug+ physical variants: `1`
- Jan metadata variants: `1`
- Aug+ metadata variants: `1`
- Jan writer variants: `1`
- Aug+ writer variants: `1`
- Jan-only columns: `['cum_value', 'first_ask_price_best', 'first_bid_price_best', 'last_ask_price_best', 'last_bid_price_best']`
- Aug-only columns: `['ticker']`
- Type/nullability changes: `[{'name': 'trade_time', 'january_type': 'timestamp[us, tz=Asia/Seoul]', 'august_type': 'timestamp[ns]', 'january_nullable': True, 'august_nullable': True}, {'name': 'trade_volume', 'january_type': 'double', 'august_type': 'int64', 'january_nullable': True, 'august_nullable': True}]`
- Column order equal: `False`
- Representative Jan writer: `{'format_version': '1.0', 'created_by': 'Polars', 'compressions': ['LZ4'], 'encodings': ['PLAIN', 'RLE', 'RLE_DICTIONARY']}`
- Representative Aug writer: `{'format_version': '1.0', 'created_by': 'Polars', 'compressions': ['LZ4'], 'encodings': ['PLAIN', 'RLE', 'RLE_DICTIONARY']}`

