# KIS 투자자 11종 2026-01 계약 검증

- 기준: `2026-01` Parquet 스키마
- TR ID: `FHPTJ04030000`
- endpoint: `/uapi/domestic-stock/v1/quotations/inquire-investor-time-by-market`
- 결과: PASS 11 / FAIL 0

| 데이터 종류 | KIS 코드 | 현재 응답 행 | 스키마 |
|---|---|---:|---|
| `kospi` | `KSP / 0001` | 1 | PASS |
| `kosdaq` | `KSQ / 1001` | 1 | PASS |
| `futures` | `K2I / F001` | 1 | PASS |
| `call_option` | `K2I / OC01` | 1 | PASS |
| `put_option` | `K2I / OP01` | 1 | PASS |
| `mini_futures` | `MKI / F004` | 1 | PASS |
| `mini_call_option` | `MKI / OC02` | 1 | PASS |
| `mini_put_option` | `MKI / OP02` | 1 | PASS |
| `kosdaq150_futures` | `KQI / F002` | 1 | PASS |
| `kosdaq150_call_option` | `KQI / OC03` | 1 | PASS |
| `kosdaq150_put_option` | `KQI / OP03` | 1 | PASS |
