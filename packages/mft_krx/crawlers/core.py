from __future__ import annotations

import datetime
import time
from io import BytesIO
from typing import Optional

import pandas as pd
import requests
from pandas import DataFrame, Timestamp

from mft_krx.auth import DEFAULT_REFERER, DEFAULT_USER_AGENT, get_krx_session
from mft_krx.config import get_config

sec_code_info: Optional[DataFrame] = None


def _get_column_names():
    """Get column name mapping from config."""
    return get_config().column_names


def to_timestamp(date: pd.Timestamp | str) -> Timestamp:
    ts = Timestamp(date)
    if pd.isna(ts):
        raise ValueError(f"Invalid date: {date}")
    return ts


# =============================================================================
# Configuration-based settings (lazy loaded)
# =============================================================================

def _get_http_config():
    """Get HTTP config from global config."""
    return get_config().crawl.http


def _get_retry_config():
    """Get retry config from global config."""
    return get_config().crawl.retry


def _get_encoding():
    """Get encoding from global config."""
    return get_config().crawl.encoding


def _to_float(series: pd.Series) -> pd.Series:
    """Convert KRX numeric text, preserving placeholders as nulls."""
    cleaned = series.astype(str).str.replace(',', '', regex=False)
    cleaned = cleaned.replace({'-': pd.NA, '': pd.NA, 'nan': pd.NA})
    return pd.to_numeric(cleaned, errors='coerce').astype('float64')


# =============================================================================
# CSV API (OTP + CSV download)
# =============================================================================

def _request_data(query_sta_params, skip_empty=True):
    """Request data from KRX with retry logic (CSV method).

    Uses configuration for retry count and delay.
    """
    retry_config = _get_retry_config()
    retry_cnt = retry_config.max_count
    delay = retry_config.delay_seconds
    last_error = None

    for i in range(retry_cnt):
        try:
            data = _request_data_once(query_sta_params)
        except Exception as e:
            last_error = e
            print(f'retry {i + 1}')
            print(e)
            time.sleep(delay)
            continue

        if len(data) > 0 or skip_empty:
            return data
        else:
            print(f'retry {i + 1}')
            print('data is empty')

        if i >= retry_cnt - 1:
            raise Exception('retry count exceeded')

        time.sleep(delay)

    if last_error is not None:
        url = query_sta_params.get("url", "unknown")
        raise RuntimeError(f"KRX CSV request failed after {retry_cnt} attempts: {url}") from last_error

    raise RuntimeError(f"KRX CSV request failed after {retry_cnt} attempts")


def _request_data_once(query_sta_params):
    """Make a single request to KRX.

    Uses OTP + CSV download API.
    """
    http_config = _get_http_config()
    encoding = _get_encoding()
    session = get_krx_session()

    r1 = session.get(
        http_config.gen_url,
        params=query_sta_params,
        headers=http_config.get_header_otp(),
        timeout=30,
    )
    r1.raise_for_status()
    otp_code = r1.text.strip()
    if not otp_code:
        raise RuntimeError("KRX OTP response is empty")
    if otp_code.upper() == "LOGOUT":
        raise RuntimeError(
            "KRX returned LOGOUT while generating OTP. "
            "Check KRX_ID/KRX_PW and the KRX login policy."
        )

    form_data = {'code': otp_code}
    r2 = session.post(
        http_config.download_url,
        data=form_data,
        headers=http_config.get_header_download(),
        timeout=30,
    )
    r2.raise_for_status()
    if not r2.content:
        raise RuntimeError("KRX CSV download returned an empty response")

    # 종목코드를 문자열로 읽어서 '00341A' 같은 영문 포함 코드도 처리
    data = pd.read_csv(BytesIO(r2.content), encoding=encoding, dtype={'종목코드': str})
    return data


# =============================================================================
# JSON API (getJsonData.cmd) - 표준코드(ISU_CD) 포함
# =============================================================================

def _request_data_json(query_sta_params, skip_empty=True):
    """Request data from KRX with retry logic (JSON method).

    Uses JSON API which provides ISU_CD (표준코드/ISIN) field.
    """
    retry_config = _get_retry_config()
    retry_cnt = retry_config.max_count
    delay = retry_config.delay_seconds
    last_error = None

    for i in range(retry_cnt):
        try:
            data = _request_data_once_json(query_sta_params)
        except Exception as e:
            last_error = e
            print(f'retry {i + 1}')
            print(e)
            time.sleep(delay)
            continue

        if len(data) > 0 or skip_empty:
            return data
        else:
            print(f'retry {i + 1}')
            print('data is empty')

        if i >= retry_cnt - 1:
            raise Exception('retry count exceeded')

        time.sleep(delay)

    if last_error is not None:
        url = query_sta_params.get("url", "unknown")
        raise RuntimeError(f"KRX JSON request failed after {retry_cnt} attempts: {url}") from last_error

    raise RuntimeError(f"KRX JSON request failed after {retry_cnt} attempts")


def _request_data_once_json(query_sta_params):
    """Make a single JSON API request to KRX.

    Uses JSON API (getJsonData.cmd) instead of CSV download.
    JSON API provides ISU_CD (표준코드/ISIN) field.
    """
    session = get_krx_session()
    url = 'https://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd'
    headers = {"User-Agent": DEFAULT_USER_AGENT, 'Referer': DEFAULT_REFERER}
    params = {
        'bld': query_sta_params['url'],
        'locale': 'ko_KR',
        **{k: v for k, v in query_sta_params.items() if k not in ['url', 'name', 'csvxls_isNo']}
    }

    res = session.post(url, headers=headers, data=params, timeout=30)
    res.raise_for_status()
    json_data = res.json()
    if json_data == "LOGOUT" or json_data.get("_error_code") == "LOGOUT":
        raise RuntimeError("KRX returned LOGOUT for JSON request")

    # 응답 키 찾기 (OutBlock_1 또는 output)
    if 'OutBlock_1' in json_data:
        data_list = json_data['OutBlock_1']
    elif 'output' in json_data:
        data_list = json_data['output']
    else:
        return DataFrame()

    if not data_list:
        return DataFrame()

    df = pd.DataFrame(data_list)

    # 컬럼명 매핑
    column_names = _get_column_names()
    df = df.rename(columns={k: v for k, v in column_names.items() if k in df.columns})

    return df


# =============================================================================
# Helper functions
# =============================================================================

def _insert_date(data: DataFrame, date: Timestamp):
    """date 컬럼을 맨 앞에 추가"""
    cols = data.columns.tolist()
    cols.insert(0, 'date')
    data['date'] = date
    data = data[cols]
    return data


def _pad_stock_code(data: DataFrame) -> DataFrame:
    """종목코드를 6자리 제로패딩 문자열로 변환

    예: "20" → "000020", "5930" → "005930", "341A" → "00341A"
    """
    if '종목코드' in data.columns:
        data['종목코드'] = data['종목코드'].astype(str).str.zfill(6)
    return data


def _join_sec_code(data: DataFrame) -> DataFrame:
    """sec_code를 종목코드 기준으로 join하고, date 오른쪽에 배치

    상장/상장폐지 전종목 정보에서 sec_code(ISIN)를 가져와 join합니다.
    """
    if data.empty or '종목코드' not in data.columns:
        return data

    sec_info = get_sec_code_info()[['종목코드', 'sec_code']]
    data = data.merge(sec_info, on='종목코드', how='left')

    # sec_code를 date 오른쪽에 배치
    cols = data.columns.tolist()
    cols.remove('sec_code')
    if 'date' in cols:
        date_idx = cols.index('date')
        cols.insert(date_idx + 1, 'sec_code')
    else:
        cols.insert(0, 'sec_code')
    data = data[cols]

    return data


# =============================================================================
# 크롤링 함수들 (JSON API 사용 - 표준코드 포함)
# =============================================================================

def 전종목시세_12001(date: pd.Timestamp | str) -> DataFrame:
    """
    화면번호 12001. 상폐일봉 받아오기 (JSON API - 표준코드 포함)
    상폐: ㅇ
    최대범위: 2007-01-02
    종목코드, 종목명, 시장구분, 소속부, 종가, 대비, 등락률, 시가, 고가, 저가, 거래량, 거래대금, 시가총액, 상장주식수

    Parameters
    ----------
    date : pd.Timestamp
        받아올 데이터의 날짜

    Returns
    -------
    df : DataFrame
        일별 전종목 시세

    Examples
    --------
    전종목시세_12001('20220608')
    """
    date = to_timestamp(date)
    if date < to_timestamp('2007-01-02'):
        date = to_timestamp('2007-01-02')

    query_sta_params = {
        'mktId': 'ALL',
        'trdDd': date.strftime('%Y%m%d'),
        'share': '1',
        'money': '1',
        'csvxls_isNo': 'false',
        'name': 'fileDown',
        'url': 'dbms/MDC/STAT/standard/MDCSTAT01501'
    }

    data = _request_data_json(query_sta_params)
    data = _pad_stock_code(data)
    data = _insert_date(data, date)

    if data.empty:
        return data

    # 시장구분 값 정규화
    data['시장구분'] = data['시장구분'].replace({'KSQ': 'KOSDAQ', 'STK': 'KOSPI'})

    # 타입 변환 (콤마 제거 후 float64)
    for col in ['종가', '대비', '시가', '고가', '저가', '거래량', '거래대금', '시가총액', '상장주식수']:
        if col in data.columns and data[col].dtype == 'object':
            data[col] = _to_float(data[col])

    if '등락률' in data.columns and data['등락률'].dtype == 'object':
        data['등락률'] = _to_float(data['등락률'])

    # 필요한 컬럼만 선택
    target_columns = [
        'date', 'sec_code', '종목코드', '종목명', '시장구분', '소속부',
        '종가', '대비', '등락률', '시가', '고가', '저가',
        '거래량', '거래대금', '시가총액', '상장주식수'
    ]
    data = data[[c for c in target_columns if c in data.columns]]

    return data


def etf전종목시세_13101(date: pd.Timestamp | str) -> DataFrame:
    """
    화면번호 13101. ETF 전종목 일별 시세 (JSON API - 표준코드 포함)
    상폐: ㅇ
    최대범위: 2002-10-14

    Parameters
    ----------
    date : pd.Timestamp

    Returns
    -------
    df : DataFrame
        ETF 전종목 일별 시세
        컬럼: date, 종목코드, sec_code, 증권그룹ID, 종목명, 종가, 대비, 등락구분, 등락률,
              NAV, 시가, 고가, 저가, 거래량, 거래대금, 시가총액, 순자산총액, 상장주식수,
              기초지수명, 기초지수종가, 기초지수대비, 기초지수등락구분, 기초지수등락률

    Examples
    -------
    etf전종목시세_13101('20220608')
    """
    date = to_timestamp(date)
    if date < to_timestamp('2002-10-14'):
        date = to_timestamp('2002-10-14')

    query_sta_params = {
        'trdDd': date.strftime('%Y%m%d'),
        'share': '1',
        'money': '1',
        'csvxls_isNo': 'false',
        'name': 'fileDown',
        'url': 'dbms/MDC/STAT/standard/MDCSTAT04301'
    }
    data = _request_data_json(query_sta_params)
    data = _pad_stock_code(data)
    data = _insert_date(data, date)

    # 타입 변환 (콤마 제거 후 float64)
    int_cols = ['종가', '대비', '시가', '고가', '저가', '거래량', '거래대금', '시가총액', '순자산총액', '상장주식수']
    float_cols = ['등락률', 'NAV', '기초지수종가', '기초지수대비', '기초지수등락률']
    for col in int_cols + float_cols:
        if col in data.columns and data[col].dtype == 'object':
            data[col] = _to_float(data[col])

    # 필요한 컬럼만 선택 (한글 컬럼명 기준)
    target_cols = ['date', '종목코드', 'sec_code', '증권그룹ID', '종목명', '종가', '대비', '등락구분', '등락률',
                   'NAV', '시가', '고가', '저가', '거래량', '거래대금', '시가총액', '순자산총액', '상장주식수',
                   '기초지수명', '기초지수종가', '기초지수대비', '기초지수등락구분', '기초지수등락률']
    data = data[[col for col in target_cols if col in data.columns]]

    return data


# =============================================================================
# 크롤링 함수들 (CSV API 사용)
# =============================================================================

def 전종목등락률_12002(date: pd.Timestamp | str) -> DataFrame:
    """
    화면번호 12002, 기준가 데이터
    상폐: ㅇ
    최대범위: 1995-05-02
    종목코드, 종목명, 시작일 기준가, 종료일 종가, 등락구분코드, 대비, 등락률, 거래량, 거래대금

    Parameters
    ----------
    date : pd.Timestamp
        받아올 데이터의 날짜

    Returns
    -------
    df : DataFrame
        일별 전종목 등락률

    Examples
    --------
    전종목등락률_12002('20220608')
    """
    date = to_timestamp(date)
    if date < to_timestamp('1995-05-02'):
        return DataFrame()

    def _gen_query_sta_params(mktId):
        query_sta_params = {
            'mktId': mktId,
            'strtDd': date.strftime('%Y%m%d'),
            'endDd': date.strftime('%Y%m%d'),
            'adjStkPrc': '1',
            'share': '1',
            'money': '1',
            'csvxls_isNo': 'false',
            'name': 'fileDown',
            'url': 'dbms/MDC/STAT/standard/MDCSTAT01602'
        }
        return query_sta_params

    코스피 = ['STK', 'kospi']
    코스닥 = ['KSQ', 'kosdaq']
    super_x = []
    for market_id, market_name in [코스피, 코스닥]:
        params = _gen_query_sta_params(market_id)
        temp = _request_data(params)
        temp['market'] = market_name
        super_x.append(temp)
        time.sleep(3)

    data = pd.concat(super_x, ignore_index=True)
    data = _pad_stock_code(data)
    data = data.sort_values('종목코드', ignore_index=True)
    data = _insert_date(data, date)
    data = _join_sec_code(data)
    return data


def 외국인보유량_개별_12023(date: pd.Timestamp) -> DataFrame:
    """
    상폐: ㅇ
    최대범위: 2005-10-04
    종목코드, 종목명, 종가, 대비, 등락률, 상장주식수, 외국인 보유수량, 외국인 지분율, 외국인 한도수량, 외국인 한도소진율

    Parameters
    ----------
    date : pd.Timestamp

    Returns
    -------
    df : DataFrame

    Examples
    --------
    외국인보유량_개별_12023('20220608')
    """
    date = to_timestamp(date)
    if date < to_timestamp('2005-10-04'):
        date = to_timestamp('2005-10-04')

    def _gen_query_sta_params(mktId):
        query_sta_params = {
            'searchType': 1,
            'mktId': mktId,
            'trdDd': date.strftime('%Y%m%d'),
            'tboxisuCd_finder_stkisu0_8': '005930/삼성전자',
            'isuCd': 'KR7005930003',
            'isuCd2': 'KR7005930003',
            'codeNmisuCd_finder_stkisu0_8': '삼성전자',
            'param1isuCd_finder_stkisu0_8': 'STK',
            'strtDd': '20210111',
            'endDd': '20210118',
            'share': '1',
            'csvxls_isNo': 'false',
            'name': 'fileDown',
            'url': 'dbms/MDC/STAT/standard/MDCSTAT03701'
        }
        return query_sta_params

    코스피 = ['STK', 'kospi']
    코스닥 = ['KSQ', 'kosdaq']
    super_x = []
    for market_id, market_name in [코스피, 코스닥]:
        params = _gen_query_sta_params(market_id)
        temp = _request_data(params)
        temp['market'] = market_name
        super_x.append(temp)
        time.sleep(3)

    data = pd.concat(super_x, ignore_index=True)
    data = _pad_stock_code(data)
    data = data.sort_values('종목코드', ignore_index=True)
    data = _insert_date(data, date)
    data = _join_sec_code(data)
    return data


def 업종별분포_12024(date: pd.Timestamp | str) -> DataFrame:
    """
    상폐: ㅇ
    최대범위: 2001-01-02
    업종명, 회사수, 종목수, 상장주식수, 시가총액, 시가총액 비율, 거래량, 거래량 비율, 거래대금, 거래대금 비율, 시장구분

    Parameters
    ----------
    date : pd.Timestamp

    Returns
    -------
    df : DataFrame

    Examples
    --------
    업종별분포_12024('20220608')
    """
    date = to_timestamp(date)
    if date < to_timestamp('1995-05-02'):
        date = to_timestamp('1995-05-02')

    def _gen_query_sta_params(mktId, idxIndCd):
        query_sta_params = {
            'searchType': 1,
            'mktId': mktId,
            'trdDd': date.strftime('%Y%m%d'),
            'idxIndCd': idxIndCd,
            'strtDd': '20210111',
            'endDd': '20210118',
            'share': '1',
            'money': '1',
            'csvxls_isNo': 'false',
            'name': 'fileDown',
            'url': 'dbms/MDC/STAT/standard/MDCSTAT03801'
        }
        return query_sta_params

    코스피 = ['STK', '005', 'kospi']
    코스닥 = ['KSQ', '021', 'kosdaq']
    super_x = []
    for market_id, market_num, market_name in [코스피, 코스닥]:
        params = _gen_query_sta_params(market_id, market_num)
        temp = _request_data(params)
        temp['market'] = market_name
        super_x.append(temp)
        time.sleep(3)

    data = pd.concat(super_x, ignore_index=True)
    data = _insert_date(data, date)
    return data


def 업종분류현황_12025(date: pd.Timestamp | str) -> DataFrame:
    """
    상폐: ㅇ
    최대범위: 2001-01-02
    종목코드, 종목명, 시장구분, 업종대분류, 업종중분류

    Parameters
    ----------
    date : pd.Timestamp

    Returns
    -------
    df : DataFrame

    Examples
    --------
    업종분류현황_12025('20220608')
    """
    date = to_timestamp(date)
    if date < to_timestamp('2001-01-02'):
        date = to_timestamp('2001-01-02')

    def _gen_query_sta_params(mktId):
        query_sta_params = {
            'mktId': mktId,
            'trdDd': date.strftime('%Y%m%d'),
            'money': '1',
            'csvxls_isNo': 'false',
            'name': 'fileDown',
            'url': 'dbms/MDC/STAT/standard/MDCSTAT03901'
        }
        return query_sta_params

    코스피 = ['STK']
    코스닥 = ['KSQ']
    super_x = []
    for market_id in [코스피, 코스닥]:
        params = _gen_query_sta_params(market_id)
        temp = _request_data(params)
        super_x.append(temp)
        time.sleep(3)

    data = pd.concat(super_x, ignore_index=True)
    data = _pad_stock_code(data)
    data = _insert_date(data, date)
    data = _join_sec_code(data)
    return data


def etf전종목등락률_13102(date: pd.Timestamp | str) -> DataFrame:
    """
    상폐: ㅇ
    최대범위: 2002-10-14
    종목코드, 종목명, 시작일 기준가, 종료일 종가, 등락구분코드, 대비, 등락률, 거래량, 거래대금

    Parameters
    ----------
    date : pd.Timestamp

    Returns
    -------
    df : DataFrame

    Examples
    -------
    etf전종목등락률_13102('20220608')
    """
    date = to_timestamp(date)
    if date < to_timestamp('2002-10-14'):
        return DataFrame()

    query_sta_params = {
        'strtDd': date.strftime('%Y%m%d'),
        'endDd': date.strftime('%Y%m%d'),
        'share': '1',
        'money': '1',
        'csvxls_isNo': 'false',
        'name': 'fileDown',
        'url': 'dbms/MDC/STAT/standard/MDCSTAT04401'
    }
    data = _request_data(query_sta_params)
    data = _pad_stock_code(data)
    data = _insert_date(data, date)
    data['market'] = 'kospi'
    data = _join_sec_code(data)
    return data


def 코스피지수시세추이_11003(
        start_date: pd.Timestamp | datetime.date | str,
        end_date: pd.Timestamp | datetime.date | str
) -> DataFrame:
    """
    상폐: ㅇ
    최대범위: 1980-01-04
    일자, 종가, 대비, 등락률, 시가, 고가, 저가, 거래량, 거래대금, 상장시가총액

    Parameters
    ----------

    Returns
    -------
    df : DataFrame

    Examples
    -------
    코스피지수시세추이_11003('20220608')
    """
    print('1994-10-29까지의 시가, 고가, 저가 컬럼은 종가의 값을 복제해 생성합니다')

    start_date = to_timestamp(start_date)
    end_date = to_timestamp(end_date)
    original_start_date = start_date
    if start_date <= to_timestamp('1994-10-31'):
        start_date -= pd.Timedelta(days=30)
        cut = True
    else:
        cut = False
    if start_date < to_timestamp('1980-01-04'):
        start_date = to_timestamp('1980-01-04')

    query_sta_params = {
        'tboxindIdx_finder_equidx0_0': '코스피',
        'indIdx': '1',
        'indIdx2': '001',
        'codeNmindIdx_finder_equidx0_0': '코스피',
        'param1indIdx_finder_equidx0_0': '',
        'strtDd': start_date.strftime('%Y%m%d'),
        'endDd': end_date.strftime('%Y%m%d'),
        'share': '1',
        'money': '1',
        'csvxls_isNo': 'false',
        'name': 'fileDown',
        'url': 'dbms/MDC/STAT/standard/MDCSTAT00301'
    }
    data = _request_data(query_sta_params)
    # 일자 컬럼을 datetime으로 변환 (비교 연산 전에 먼저 수행)
    data['일자'] = pd.to_datetime(data['일자'])
    dates = data['일자'].tolist()
    if start_date <= to_timestamp('1994-10-31'):
        close_nan = data.loc[data['일자'] <= to_timestamp('1994-10-31'), '종가']
        data.loc[data['일자'] <= to_timestamp('1994-10-31'), '대비'] = close_nan - close_nan.shift(1)
        data.loc[data['일자'] <= to_timestamp('1994-10-31'), '등락률'] = (close_nan / close_nan.shift(1) - 1) * 100

        if start_date <= to_timestamp('1980-01-04'):
            data.loc[0, '대비'] = 0
            data.loc[0, '등락률'] = 0

        if start_date <= to_timestamp('1994-10-31'):
            data.loc[data['일자'] <= to_timestamp('1994-10-29'), ['시가', '고가', '저가']] = \
                data.loc[data['일자'] <= to_timestamp('1994-10-29'), '종가']

        if cut:
            data = data.loc[data['일자'] >= original_start_date, :]

        if to_timestamp('1994-11-14') in dates:
            data.loc[data['일자'] == to_timestamp('1994-11-14'), '대비'] = 12.34
            data.loc[data['일자'] == to_timestamp('1994-11-14'), '등락률'] = 1.11

        if to_timestamp('1994-11-19') in dates:
            data.loc[data['일자'] == to_timestamp('1994-11-19'), '거래대금'] = 740905000000

        if to_timestamp('1994-11-23') in dates:
            data.loc[data['일자'] == to_timestamp('1994-11-23'), '시가'] = 1108.82

        if to_timestamp('1994-12-27') in dates:
            data.loc[data['일자'] == to_timestamp('1994-12-27'), '저가'] = 1017.97

    # 컬럼명 변경 (다른 데이터와 동일한 형식)
    data = data.rename(columns={'일자': 'date'})
    # date 컬럼을 맨 앞으로
    cols = ['date'] + [c for c in data.columns if c != 'date']
    data = data[cols]

    # 타입 일관성을 위해 float 컬럼 명시적 변환
    float_cols = ['종가', '대비', '등락률', '시가', '고가', '저가', '상장시가총액']
    for col in float_cols:
        if col in data.columns:
            data[col] = data[col].astype(float)

    return data


def 지수구성종목_11006(
        date: Timestamp | str,
        index_name: str,
        market: str,
        ind_id: str,
) -> DataFrame:
    """
    화면번호 11006. 지수구성종목 받아오기
    상폐: ㅇ
    최대범위: 2004-01-02 (불확실)
    종목코드, 종목명, 시장구분, 소속부, 종가, 대비, 등락률

    Parameters
    ----------
    date : pd.Timestamp
        받아올 데이터의 날짜

    index_name : str
        받아올 데이터의 이름

    market : str
        시장구분

    ind_id : str
        krx 상에서 정의된 인덱스의 고유 id

    Returns
    -------
    df : DataFrame
        지수구성종목

    Examples
    -------
    지수구성종목_11006('20220608')
    """
    date = to_timestamp(date)
    if date < to_timestamp('1980-01-04'):
        date = to_timestamp('1980-01-04')

    query_sta_params = {
        'locale': 'ko_KR',
        'tboxindIdx_finder_equidx0_0': index_name,
        'indIdx': market,
        'indIdx2': ind_id,
        'codeNmindIdx_finder_equidx0_0': index_name,
        'param1indIdx_finder_equidx0_0': '',
        'trdDd': date.strftime('%Y%m%d'),
        'money': '1',
        'csvxls_isNo': 'false',
        'name': 'fileDown',
        'url': 'dbms/MDC/STAT/standard/MDCSTAT00601',
    }
    data = _request_data(query_sta_params)
    data = _pad_stock_code(data)
    data = _join_sec_code(data)
    data = _insert_date(data, date)
    return data


def 지수목록_조회():
    """
    KRX에서 전체 지수 목록을 조회하는 함수
    Returns: DataFrame 또는 List[dict]
    """
    http_config = _get_http_config()
    headers = {
        "User-Agent": http_config.user_agent,
        "Referer": http_config.referer
    }
    params = {
        'locale': 'ko_KR',
        'mktsel': '1',
        'searchText': '',
        'bld': 'dbms/comm/finder/finder_equidx',
    }
    res = get_krx_session().post(
        http_config.json_url,
        headers=headers,
        data=params,
        timeout=30,
    )
    return res.json()['block1']  # 전체 지수 목록


# =============================================================================
# 투자자별 거래실적 (KRX bulk JSON API)
# =============================================================================

INVESTOR_TYPE_CODES = {
    '금융투자': '1000',
    '보험': '2000',
    '투신': '3000',
    '사모': '3100',
    '은행': '4000',
    '기타금융': '5000',
    '연기금': '6000',
    '기타법인': '7100',
    '개인': '8000',
    '외국인': '9000',
    '기타외국인': '9001',
    '전체': '9999',
}


def _투자자별순매수종목(date: Timestamp, investor_name: str, investor_code: str) -> DataFrame:
    """Return one investor type's per-symbol net trading value in KRW millions."""
    date_str = date.strftime('%Y%m%d')
    frame = _request_data_json(
        {
            'url': 'dbms/MDC/STAT/standard/MDCSTAT02401',
            'strtDd': date_str,
            'endDd': date_str,
            'mktId': 'ALL',
            'invstTpCd': investor_code,
            'share': '1',
            'money': '1',
        },
        skip_empty=False,
    )
    required = {'종목코드', 'NETBID_TRDVAL'}
    missing = required - set(frame.columns)
    if missing:
        raise RuntimeError(
            f"KRX investor response missing columns for {investor_name}: {sorted(missing)}"
        )

    values = _to_float(frame['NETBID_TRDVAL']) / 1_000_000
    return DataFrame(
        {
            '종목코드': frame['종목코드'].astype(str).str.zfill(6),
            investor_name: values.fillna(0).astype('int64'),
        }
    ).drop_duplicates(subset=['종목코드'], keep='last')


def 투자자별거래실적개별종목_12010(date: pd.Timestamp | str) -> DataFrame:
    """Collect per-symbol investor net trading values using 12 bulk KRX calls.

    The former pykrx implementation first requested a ticker list and then
    issued one request per symbol.  KRX now returns an empty ticker list through
    that pykrx path, and a full run would also require thousands of requests.
    MDCSTAT02401 returns every ranked symbol for one investor type in a single
    response, so twelve investor-type requests cover the same detailed columns.

    Symbols present in the KOSPI/KOSDAQ daily universe but absent from a ranking
    response are retained with zero net trading value.  Values are stored in
    KRW millions for compatibility with the previous output.
    """
    target = to_timestamp(date)
    universe = 전종목시세_12001(target)
    required_universe = {'date', 'sec_code', '종목코드', '종목명', '시장구분'}
    missing = required_universe - set(universe.columns)
    if missing:
        raise RuntimeError(f"KRX stock universe missing columns: {sorted(missing)}")

    data = (
        universe.loc[universe['시장구분'].isin(['KOSPI', 'KOSDAQ'])]
        [['date', 'sec_code', '종목코드', '종목명']]
        .copy()
    )
    if data.empty:
        raise RuntimeError(f"KRX stock universe is empty for {target.date()}")
    data['종목코드'] = data['종목코드'].astype(str).str.zfill(6)
    data = data.drop_duplicates(subset=['종목코드'], keep='last')

    for index, (investor_name, investor_code) in enumerate(INVESTOR_TYPE_CODES.items()):
        if index:
            time.sleep(0.5)
        investor = _투자자별순매수종목(target, investor_name, investor_code)
        data = data.merge(investor, on='종목코드', how='left')
        data[investor_name] = data[investor_name].fillna(0).astype('int64')

    columns = [
        'date', 'sec_code', '종목코드', '종목명',
        *INVESTOR_TYPE_CODES.keys(),
    ]
    return data[columns].sort_values('종목코드').reset_index(drop=True)


def 투자자별거래실적개별종목_pykrx(date: pd.Timestamp | str) -> DataFrame:
    """Backward-compatible alias for the KRX bulk implementation."""
    return 투자자별거래실적개별종목_12010(date)


# =============================================================================
# 상장/상폐 종목 조회 (JSON API)
# =============================================================================

def 상장종목(market: str = 'ALL') -> DataFrame:
    """
    KRX에서 상장종목 목록을 조회하는 함수

    Parameters
    ----------
    market : str
        시장구분 ('ALL', 'STK'(코스피), 'KSQ'(코스닥), 'KNX'(코넥스))
        기본값: 'ALL' (전체)

    Returns
    -------
    df : DataFrame
        상장종목 목록
        컬럼: sec_code(ISIN), 종목코드, 종목명, market(시장구분)

    Examples
    --------
    >>> 상장종목()  # 전체 시장
    >>> 상장종목('STK')  # 코스피만
    >>> 상장종목('KSQ')  # 코스닥만
    """
    http_config = _get_http_config()

    headers = http_config.get_header_json(
        referer="https://data.krx.co.kr/contents/MDC/MDI/outerLoader/index.cmd?menuId=MDC0201"
    )
    form_data = {
        'locale': 'ko_KR',
        'mktsel': market,
        'typeNo': '0',
        'searchText': '',
        'bld': 'dbms/comm/finder/finder_stkisu',
    }

    res = get_krx_session().post(
        http_config.json_url,
        headers=headers,
        data=form_data,
        timeout=30,
    )
    res.raise_for_status()

    json_data = res.json()
    if 'block1' not in json_data:
        return DataFrame()

    data = DataFrame(json_data['block1'])
    if data.empty:
        return data

    # 종목코드 6자리 제로패딩
    if 'short_code' in data.columns:
        data['short_code'] = data['short_code'].astype(str).str.zfill(6)

    # 컬럼명 변경 및 필요한 컬럼만 선택
    data = data.rename(columns={
        'full_code': 'sec_code',
        'short_code': '종목코드',
        'codeName': '종목명',
        'marketCode': 'market',
    })
    data = data[['sec_code', '종목코드', '종목명', 'market']]

    # market 값 변환
    market_map = {
        'KSQ': 'kosdaq',
        'STK': 'kospi',
        'KNX': 'konex',
    }
    data['market'] = data['market'].replace(market_map)

    return data


def 상장폐지종목(market: str = 'ALL') -> DataFrame:
    """
    KRX에서 상장폐지종목 목록을 조회하는 함수

    Parameters
    ----------
    market : str
        시장구분 ('ALL', 'STK'(코스피), 'KSQ'(코스닥), 'KNX'(코넥스))
        기본값: 'ALL' (전체)

    Returns
    -------
    df : DataFrame
        상장폐지종목 목록
        컬럼: sec_code(ISIN), 종목코드, 종목명, market(시장구분)

    Examples
    --------
    >>> 상장폐지종목()  # 전체 시장
    >>> 상장폐지종목('STK')  # 코스피만
    >>> 상장폐지종목('KSQ')  # 코스닥만
    """
    http_config = _get_http_config()

    headers = http_config.get_header_json(
        referer="https://data.krx.co.kr/contents/MDC/MDI/outerLoader/index.cmd?menuId=MDC02021301"
    )
    form_data = {
        'locale': 'ko_KR',
        'mktsel': market,
        'searchText': '',
        'bld': 'dbms/comm/finder/finder_listdelisu',
    }

    res = get_krx_session().post(
        http_config.json_url,
        headers=headers,
        data=form_data,
        timeout=30,
    )
    res.raise_for_status()

    json_data = res.json()
    if 'block1' not in json_data:
        return DataFrame()

    data = DataFrame(json_data['block1'])
    if data.empty:
        return data

    # 종목코드 6자리 제로패딩
    if 'short_code' in data.columns:
        data['short_code'] = data['short_code'].astype(str).str.zfill(6)

    # 컬럼명 변경 및 필요한 컬럼만 선택
    data = data.rename(columns={
        'full_code': 'sec_code',
        'short_code': '종목코드',
        'codeName': '종목명',
        'marketCode': 'market',
    })
    data = data[['sec_code', '종목코드', '종목명', 'market']]

    # market 값 변환
    market_map = {
        'KSQ': 'kosdaq',
        'STK': 'kospi',
        'KNX': 'konex',
    }
    data['market'] = data['market'].replace(market_map)

    return data


def 상장상폐전종목() -> DataFrame:
    listed = 상장종목()
    delisted = 상장폐지종목()
    return pd.concat([listed, delisted], ignore_index=True)


# =============================================================================
# 파생상품 크롤링 함수들 (JSON API)
# =============================================================================

# 파생상품 상품ID 매핑 (prodId prefix가 KR___로 변경됨 - 2025년 5월경)
DERIVATIVE_PRODUCTS = {
    'KR___FUK2I': '코스피200선물',
    'KR___FUMKI': '미니코스피200선물',
    'KR___OPK2I': '코스피200옵션',
    'KR___OPWKI': '코스피200위클리옵션목',
    'KR___OPWKM': '코스피200위클리옵션월',
    'KR___FUKQI': '코스닥선물',
    'KR___FUXI3': 'KRX300선물',
    'KR___FUVKI': '변동성지수선물',
    'KR___FUXAT': '섹터지수선물',
    'KR___FUBM3': '3년국채선물',
    'KR___FUBM5': '5년국채선물',
    'KR___FUBMA': '10년국채선물',
    'KR___FUUSD': '미국달러선물',
    'KR___FUJPY': '엔선물',
    'KR___FUEUR': '유로선물',
    'KR___FUCNH': '위안선물',
    'KR___FUKGD': '금선물',
    'KR___FUEQU': '주식선물',
}



def 파생전종목시세_12501(date: pd.Timestamp | str) -> DataFrame:
    """
    화면번호 12501. 파생상품(선물/옵션) 전종목 일별 시세 (JSON API)
    모든 파생상품 종류를 순회하며 데이터 수집
    최대범위: 2015-01-02

    Parameters
    ----------
    date : pd.Timestamp | str
        받아올 데이터의 날짜

    Returns
    -------
    df : DataFrame
        파생상품 전종목 일별 시세
        컬럼: date, sec_code, 종목코드, 종목명, 종가, 등락구분, 대비,
              시가, 고가, 저가, 현물가, 정산가,
              거래량, 거래대금, 미결제약정, 증권그룹ID

    Examples
    --------
    >>> 파생전종목시세_12501('20241210')
    """
    date = to_timestamp(date)
    if date < to_timestamp('2015-01-02'):
        return DataFrame()

    all_data = []
    is_holiday = False

    url = 'https://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd'
    headers = {"User-Agent": DEFAULT_USER_AGENT, 'Referer': DEFAULT_REFERER}

    for prodId, prod_name in DERIVATIVE_PRODUCTS.items():
        if is_holiday:
            continue

        # IP 차단 방지용 sleep
        time.sleep(3)

        params = {
            'bld': 'dbms/MDC/STAT/standard/MDCSTAT12501',
            'locale': 'ko_KR',
            'trdDd': date.strftime('%Y%m%d'),
            'prodId': prodId,
            'trdDdBox1': date.strftime('%Y%m%d'),
            'aggBasTpCd': '',
            'rghtTpCd': 'T',
            'share': '1',
            'money': '3',
            'csvxls_isNo': 'false',
        }

        try:
            res = get_krx_session().post(url, headers=headers, data=params, timeout=30)
            res.raise_for_status()
            json_data = res.json()

            if 'output' not in json_data or not json_data['output']:
                # 코스피200선물 데이터가 없으면 휴장일로 판단
                if prodId == 'KR___FUK2I':
                    is_holiday = True
                continue

            df = pd.DataFrame(json_data['output'])

            if len(df) > 0:
                all_data.append(df)

        except Exception as e:
            print(f"Error fetching {prod_name}: {e}")
            continue

    if is_holiday or not all_data:
        return DataFrame()

    data = pd.concat(all_data, ignore_index=True)

    # 컬럼명 매핑 적용 (config.yaml의 column_names 사용)
    column_names = _get_column_names()
    data = data.rename(columns={k: v for k, v in column_names.items() if k in data.columns})

    # 타입 변환 (콤마 제거 후 float64) - 거래량, 거래대금, 미결제약정만
    for col in ['거래량', '거래대금', '미결제약정']:
        if col in data.columns and data[col].dtype == 'object':
            data[col] = _to_float(data[col])

    # 필요한 컬럼만 선택 (한글 컬럼명 기준)
    target_cols = ['sec_code', '종목코드', '종목명', '종가', '등락구분', '대비',
                   '시가', '고가', '저가', '현물가', '정산가',
                   '거래량', '거래대금', '미결제약정', '증권그룹ID']
    data = data[[col for col in target_cols if col in data.columns]]

    data = _insert_date(data, date)
    return data


def 대량매매전일_12011(date: pd.Timestamp | str) -> DataFrame:
    """
    화면번호 12011 (실제 MDCSTAT02501). 전일 대량매매 내역 (JSON API)

    최대범위: 당일 데이터만 제공 (전일 대량매매)

    Parameters
    ----------
    date : pd.Timestamp | str
        받아올 데이터의 날짜 (YYYYMMDD 형식)

    Returns
    -------
    df : DataFrame
        대량매매 전일 데이터
        컬럼: date, 종목코드, 종목명, 종가, 대비, 등락률, 거래량, 대량매매수량, 대량매매비율

    Examples
    --------
    >>> 대량매매전일_12011('20251212')
    """
    date = to_timestamp(date)

    url = 'https://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd'
    headers = {"User-Agent": DEFAULT_USER_AGENT, 'Referer': DEFAULT_REFERER}
    params = {
        "bld": "dbms/MDC/STAT/standard/MDCSTAT02501",
        "locale": "ko_KR",
        "mktId": "ALL",  # ALL:전체, STK:코스피, KSQ:코스닥, KNX:코넥스
        "share": "1",    # 1:주, 2:천주, 3:백만주
        "csvxls_isNo": "false",
    }

    res = get_krx_session().post(url, headers=headers, data=params, timeout=30)
    res.raise_for_status()
    json_data = res.json()

    if 'output' not in json_data or not json_data['output']:
        return DataFrame()

    df = pd.DataFrame(json_data['output'])

    # 컬럼 선택 및 이름 변경
    col_mapping = {
        'ISU_SRT_CD': '종목코드',
        'ISU_NM': '종목명',
        'TDD_CLSPRC': '종가',
        'CMPPRVDD_PRC': '대비',
        'FLUC_RT': '등락률',
        'ACC_TRDVOL': '거래량',
        'BLK_TRDVOL': '대량매매수량',
        'BLK_TRD_RTO': '대량매매비율',
    }

    # 필요한 컬럼만 선택
    df = df[[col for col in col_mapping.keys() if col in df.columns]]
    df = df.rename(columns=col_mapping)

    # 휴장일 체크 (종가가 모두 같으면 휴장일)
    if len(df['종가'].unique()) == 1:
        return DataFrame()

    # 숫자 컬럼 변환 (콤마 제거 후 타입 변환)
    numeric_cols = {
        '종가': 'int64',
        '대비': 'int64',
        '등락률': 'float64',
        '거래량': 'int64',
        '대량매매수량': 'int64',
        '대량매매비율': 'float64',
    }

    for col, dtype in numeric_cols.items():
        if col in df.columns:
            df[col] = df[col].str.replace(',', '').astype(dtype)

    # 날짜 컬럼 추가
    df = _insert_date(df, date)

    return df


def setup_proxy():
    """No proxy needed in local environment."""
    pass


def get_sec_code_info():
    global sec_code_info
    setup_proxy()
    if sec_code_info is None:
        sec_code_info = 상장상폐전종목()
    return sec_code_info


if __name__ == "__main__":
    setup_proxy()
    # a = 상장폐지종목()
    # b = 상장종목()
    # c = 전종목등락률_12002("20251201")
    # all_ = 상장상폐전종목()
    print(전종목시세_12001("20251201"))
    # k = 코스피지수시세추이_11003("20251202", "20251202")
