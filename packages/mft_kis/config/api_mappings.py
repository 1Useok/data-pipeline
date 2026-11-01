"""
API TR_ID mappings and parameter definitions
"""
from typing import Dict, List, Any, Optional
from dataclasses import dataclass


@dataclass
class APIParameter:
    """API parameter definition"""
    name: str
    korean_name: str
    required: bool
    default_value: Optional[str] = None
    description: str = ""
    valid_values: Optional[List[str]] = None


class APIMapping:
    """
    Maps TR_IDs to their parameters and metadata
    Note: Endpoints are loaded from config.yaml, not stored here
    """
    
    # Parameter definitions for each TR_ID
    PARAMETERS = {
        "FHKIF03020100": {  # 선물옵션기간별시세(일/주/월/년)
            "required": [
                APIParameter("FID_COND_MRKT_DIV_CODE", "FID 조건 시장 분류 코드", True, "F", 
                           "F:지수선물, O:지수옵션, JF:주식선물, JO:주식옵션, CF:상품선물, CM:야간선물, EU:야간옵션", 
                           ["F", "O", "JF", "JO", "CF", "CM", "EU"]),
                APIParameter("FID_INPUT_ISCD", "종목코드", True, "", "종목번호 (지수선물:6자리, 지수옵션:9자리)"),
                APIParameter("FID_INPUT_DATE_1", "조회 시작일자", True, "", "조회 시작일자 (YYYYMMDD)"),
                APIParameter("FID_INPUT_DATE_2", "조회 종료일자", True, "", "조회 종료일자 (YYYYMMDD)"),
                APIParameter("FID_PERIOD_DIV_CODE", "기간분류코드", True, "D", "D:일봉, W:주봉, M:월봉, Y:년봉", 
                           ["D", "W", "M", "Y"])
            ],
            "optional": []
        },
        
        "FHKIF03020200": {  # 선물옵션 분봉조회
            "required": [
                APIParameter("FID_COND_MRKT_DIV_CODE", "FID 조건 시장 분류 코드", True, "F",
                           "F:지수선물, O:지수옵션, JF:주식선물, JO:주식옵션, CF:상품선물, CM:야간선물, EU:야간옵션",
                           ["F", "O", "JF", "JO", "CF", "CM", "EU"]),
                APIParameter("FID_INPUT_ISCD", "FID 입력 종목코드", True, "", "종목번호 (지수선물:6자리, 지수옵션:9자리)"),
                APIParameter("FID_HOUR_CLS_CODE", "FID 시간 구분 코드", True, "60", "30:30초, 60:1분, 3600:1시간",
                           ["30", "60", "3600"]),
                APIParameter("FID_PW_DATA_INCU_YN", "FID 과거 데이터 포함 여부", True, "Y", "Y:과거, N:당일",
                           ["Y", "N"]),
                APIParameter("FID_FAKE_TICK_INCU_YN", "FID 허봉 포함 여부", True, "N", "N으로 고정",
                           ["N"]),
                APIParameter("FID_INPUT_DATE_1", "FID 입력 날짜1", True, "", "입력 날짜 (YYYYMMDD)"),
                APIParameter("FID_INPUT_HOUR_1", "FID 입력 시간1", True, "093000", "입력 시간 (HHMMSS)")
            ],
            "optional": []
        },
        
        "FHPIF05030100": {  # 국내옵션전광판_콜풋
            "required": [
                APIParameter("FID_COND_MRKT_DIV_CODE", "조건 시장 분류 코드", True, "O", "시장구분코드 (O: 옵션)",
                           ["O"]),
                APIParameter("FID_COND_SCR_DIV_CODE", "조건 화면 분류 코드", True, "20503", "Unique key(20503)"),
                APIParameter("FID_MRKT_CLS_CODE", "시장 구분 코드", True, "CO", "시장구분코드 (CO: 콜옵션)",
                           ["CO"]),
                APIParameter("FID_MTRT_CNT", "만기 수", True, "", "만기년월(YYYYMM) 또는 만기년월주차(YYMMWW)"),
                APIParameter("FID_COND_MRKT_CLS_CODE", "조건 시장 구분 코드", True, "", 
                           "공백:KOSPI200, MKI:미니KOSPI200, WKM:KOSPI200위클리(월), WKI:KOSPI200위클리(목), KQI:KOSDAQ150",
                           ["", "MKI", "WKM", "WKI", "KQI"]),
                APIParameter("FID_MRKT_CLS_CODE1", "시장 구분 코드", True, "PO", "시장구분코드 (PO: 풋옵션)",
                           ["PO"])
            ],
            "optional": []
        },
        
        "HHDFC55020400": {  # 해외선물 분봉조회
            "required": [
                APIParameter("FID_COND_MRKT_DIV_CODE", "FID 조건 시장 분류 코드", True, "N", "N: 해외선물"),
                APIParameter("FID_INPUT_ISCD", "FID 입력 종목코드", True, "", "해외선물 종목코드"),
                APIParameter("FID_HOUR_CLS_CODE", "FID 시간 구분 코드", True, "1", "1:1분, 3:3분, 5:5분, 10:10분, 15:15분, 30:30분, 60:1시간"),
                APIParameter("FID_INPUT_DATE_1", "FID 입력 날짜1", True, "", "조회 시작일자 (YYYYMMDD)"),
                APIParameter("FID_INPUT_DATE_2", "FID 입력 날짜2", True, "", "조회 종료일자 (YYYYMMDD)")
            ],
            "optional": []
        },
        
        "HHDDB95030000": {  # 해외선물 미결제추이
            "required": [
                APIParameter("FID_COND_MRKT_DIV_CODE", "FID 조건 시장 분류 코드", True, "N", "N: 해외선물"),
                APIParameter("FID_INPUT_ISCD", "FID 입력 종목코드", True, "", "해외선물 종목코드 (예: DX, CL)"),
                APIParameter("UPMU_GUBUN", "업무구분", True, "0", "0: 기본값"),
                APIParameter("BSOP_DATE", "영업일자", True, "", "조회일자 (YYYYMMDD), 공백시 최신")
            ],
            "optional": []
        },
        
        "FHPTJ04040000": {  # 시장별 투자자매매동향(일별)
            "required": [
                APIParameter("FID_COND_MRKT_DIV_CODE", "FID 조건 시장 분류 코드", True, "U", "U: 업종"),
                APIParameter("FID_INPUT_ISCD_1", "FID 입력 종목코드1", True, "KSP", "KSP: KOSPI, KSQ: KOSDAQ"),
                APIParameter("FID_INPUT_ISCD_2", "FID 입력 종목코드2", True, "0001", "업종코드 (0001: 종합지수)"),
                APIParameter("FID_INPUT_DATE_1", "FID 입력 날짜1", True, "", "조회 시작일자 (YYYYMMDD)"),
                APIParameter("FID_INPUT_DATE_2", "FID 입력 날짜2", True, "", "조회 종료일자 (YYYYMMDD)")
            ],
            "optional": []
        },
        
        "FHPTJ04030000": {  # 시장별 투자자매매동향(시간대별)
            "required": [
                APIParameter("FID_COND_MRKT_DIV_CODE", "FID 조건 시장 분류 코드", True, "U", "U: 업종"),
                APIParameter("FID_INPUT_ISCD_1", "FID 입력 종목코드1", True, "KSP", "KSP: KOSPI, KSQ: KOSDAQ"),
                APIParameter("FID_INPUT_ISCD_2", "FID 입력 종목코드2", True, "0001", "업종코드 (0001: 종합지수)"),
                APIParameter("FID_INPUT_DATE_1", "FID 입력 날짜1", True, "", "조회일자 (YYYYMMDD)")
            ],
            "optional": []
        },

        "FHKST03010230": {  # 주식일별분봉조회 (국내주식)
            "required": [
                APIParameter("FID_COND_MRKT_DIV_CODE", "조건 시장 분류 코드", True, "J", "J:KRX, NX:NXT, UN:통합",
                           ["J", "NX", "UN"]),
                APIParameter("FID_INPUT_ISCD", "입력 종목코드", True, "", "종목코드 (ex 005930 삼성전자)"),
                APIParameter("FID_INPUT_HOUR_1", "입력 시간1", True, "153000", "입력 시간 (HHMMSS, ex 153000)"),
                APIParameter("FID_INPUT_DATE_1", "입력 날짜1", True, "", "입력 날짜 (YYYYMMDD)"),
                APIParameter("FID_PW_DATA_INCU_YN", "과거 데이터 포함 여부", True, "Y", "Y:포함, N:미포함",
                           ["Y", "N"]),
                APIParameter("FID_FAKE_TICK_INCU_YN", "허봉 포함 여부", True, " ", "공백 필수")
            ],
            "optional": []
        }
    }
    
    # HTTP Methods for each TR_ID
    HTTP_METHODS = {
        "FHKIF03020100": "GET",
        "FHKIF03020200": "GET",
        "FHPIF05030100": "GET",
        "HHDFC55020400": "GET",
        "HHDDB95030000": "GET",
        "FHPTJ04040000": "GET",
        "FHPTJ04030000": "GET",
        "FHKST03010230": "GET",
        "VTFO6118R": "GET",
        "CTFO6118R": "GET",
        "VTTO1101U": "POST",
        "VTTO1103U": "POST",
        "VTTO5201R": "GET",
        "VTTO5105R": "GET",
        "FHMIF10000000": "GET"
    }
    
    # Output field mappings (for standardizing column names)
    OUTPUT_MAPPINGS = {
        "FHKIF03020100": {
            "stck_bsop_date": "date",
            "futs_prpr": "close_price",
            "futs_oprc": "open_price", 
            "futs_hgpr": "high_price",
            "futs_lwpr": "low_price",
            "acml_vol": "volume",
            "acml_tr_pbmn": "amount"
        },
        "FHKIF03020200": {
            "stck_bsop_date": "date",
            "stck_cntg_hour": "time",
            "futs_prpr": "close_price",
            "futs_oprc": "open_price",
            "futs_hgpr": "high_price", 
            "futs_lwpr": "low_price",
            "cntg_vol": "volume",
            "acml_tr_pbmn": "amount"
        },
        "FHPIF05030100": {
            "acpr": "strike_price",
            "optn_prpr": "option_price",
            "optn_oprc": "open_price",
            "optn_hgpr": "high_price",
            "optn_lwpr": "low_price",
            "acml_vol": "volume",
            "acml_tr_pbmn": "amount",
            "delta_val": "delta",
            "gama": "gamma",
            "vega": "vega",
            "theta": "theta",
            "rho": "rho",
            "hts_ints_vltl": "implied_volatility"
        },
        "FHKST03010230": {
            "stck_bsop_date": "date",
            "stck_cntg_hour": "time",
            "stck_prpr": "close_price",
            "stck_oprc": "open_price",
            "stck_hgpr": "high_price",
            "stck_lwpr": "low_price",
            "cntg_vol": "volume",
            "acml_tr_pbmn": "amount"
        }
    }
    
    # Feature name to TR_ID mapping
    FEATURE_TR_MAPPING = {
        "domestic_futures_daily": "FHKIF03020100",
        "domestic_futures_minute": "FHKIF03020200",
        "domestic_futures_board": "FHPIF05030200",
        "domestic_options_board": "FHPIF05030100",
        "domestic_options_daily": "FHKIF03020100",
        "domestic_options_minute": "FHKIF03020200",  # 옵션도 같은 분봉조회 API 사용
        "domestic_stock_minute": "FHKST03010230",  # 주식일별분봉조회
        "investor_trading_daily": "FHPTJ04040000",
        "investor_trading_time": "FHPTJ04030000",
        "overseas_futures_current": "HHDFC55010000",
        "overseas_futures_minute": "HHDFC55020400",
        "overseas_options_minute": "HHDFO55020400",
        "overseas_futures_cot": "HHDDB95030000",
        "overseas_options_tick": "HHDFO55020200"
    }
    
    @classmethod
    def get_endpoint(cls, tr_id: str, endpoints_config: Dict[str, str]) -> Optional[str]:
        """Get API endpoint for TR_ID from config"""
        return endpoints_config.get(tr_id)
        
    @classmethod
    def get_parameters(cls, tr_id: str) -> Dict[str, List[APIParameter]]:
        """Get parameter definitions for TR_ID"""
        return cls.PARAMETERS.get(tr_id, {"required": [], "optional": []})
        
    @classmethod
    def get_http_method(cls, tr_id: str) -> str:
        """Get HTTP method for TR_ID"""
        return cls.HTTP_METHODS.get(tr_id, "GET")
        
    @classmethod
    def get_output_mapping(cls, tr_id: str) -> Dict[str, str]:
        """Get output field mapping for TR_ID"""
        return cls.OUTPUT_MAPPINGS.get(tr_id, {})
        
    @classmethod
    def validate_parameters(cls, tr_id: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate and complete parameters for a TR_ID
        
        Args:
            tr_id: Transaction ID
            params: Input parameters
            
        Returns:
            Dict with validation result and completed parameters
        """
        param_defs = cls.get_parameters(tr_id)
        if not param_defs:
            return {"valid": False, "error": f"Unknown TR_ID: {tr_id}"}
            
        completed_params = params.copy()
        errors = []
        
        # Check required parameters
        for param_def in param_defs["required"]:
            param_name = param_def.name
            
            if param_name not in completed_params or completed_params[param_name] == "":
                if param_def.default_value is not None:
                    completed_params[param_name] = param_def.default_value
                else:
                    errors.append(f"Required parameter missing: {param_name} ({param_def.korean_name})")
                    
            # Validate valid values
            if (param_name in completed_params and 
                param_def.valid_values and 
                completed_params[param_name] not in param_def.valid_values):
                errors.append(f"Invalid value for {param_name}: {completed_params[param_name]}. "
                            f"Valid values: {', '.join(param_def.valid_values)}")
        
        # Add default values for optional parameters
        for param_def in param_defs.get("optional", []):
            if param_def.name not in completed_params and param_def.default_value is not None:
                completed_params[param_def.name] = param_def.default_value
        
        return {
            "valid": len(errors) == 0,
            "errors": errors,
            "parameters": completed_params
        }
        
    @classmethod
    def get_numeric_columns(cls, tr_id: str) -> List[str]:
        """Get list of numeric columns for a TR_ID"""
        numeric_mappings = {
            "FHKIF03020100": [
                "futs_prpr", "futs_oprc", "futs_hgpr", "futs_lwpr",
                "acml_vol", "acml_tr_pbmn", "hts_otst_stpl_qty"
            ],
            "FHKIF03020200": [
                "futs_prpr", "futs_oprc", "futs_hgpr", "futs_lwpr",
                "cntg_vol", "acml_tr_pbmn"
            ],
            "FHPIF05030100": [
                "acpr", "optn_prpr", "optn_oprc", "optn_hgpr", "optn_lwpr",
                "acml_vol", "acml_tr_pbmn", "hts_otst_stpl_qty",
                "delta_val", "gama", "vega", "theta", "rho", "hts_ints_vltl"
            ],
            "FHPTJ04040000": [
                "frgn_ntby_qty", "orgn_ntby_qty", "prsn_ntby_qty"
            ],
            "FHKST03010230": [
                "stck_prpr", "stck_oprc", "stck_hgpr", "stck_lwpr",
                "cntg_vol", "acml_tr_pbmn"
            ]
        }
        return numeric_mappings.get(tr_id, [])

    @classmethod
    def get_tr_id_for_feature(cls, feature_name: str) -> Optional[str]:
        """Get TR_ID for feature name"""
        return cls.FEATURE_TR_MAPPING.get(feature_name)