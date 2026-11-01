# -*- coding: utf-8 -*-
"""
시장별/투자자별 매매동향 데이터 수집기 (시간대별)
"""
import logging
from typing import Optional
import pandas as pd

from .base_fetcher import BaseFetcher

logger = logging.getLogger(__name__)

class InvestorBuyTimeFetcher(BaseFetcher):
    """
    지정된 시장에 대한 시간대별 투자자 매매동향 데이터를 수집합니다.
    이 API는 특정 시점의 데이터만 조회 가능합니다.
    """

    def __init__(self, api_client, **params):
        """
        Initialize investor trading trend time fetcher
        
        Args:
            api_client: RestApi instance
            **params: Additional parameters
                - tr_id: Transaction ID (default: FHPTJ04030000)
                - fid_input_iscd: Market code (e.g., 'KSP')
                - fid_input_iscd_2: Product code (e.g., '0001')
        """
        # 속성을 super().__init__() 호출 전에 설정
        self.fid_input_iscd = params.get("fid_input_iscd")
        self.fid_input_iscd_2 = params.get("fid_input_iscd_2")
        
        if not params.get('tr_id'):
            params['tr_id'] = "FHPTJ04030000"
            
        super().__init__(api_client, **params)
    
    def _validate_params(self):
        """Validate required parameters"""
        if not all([self.tr_id, self.fid_input_iscd, self.fid_input_iscd_2]):
            raise ValueError("필수 파라미터가 누락되었습니다: tr_id, fid_input_iscd, fid_input_iscd_2")

    async def fetch_data(self) -> Optional[pd.DataFrame]:
        """
        현재 시점의 시간대별 매매동향 데이터를 수집하여 DataFrame으로 반환합니다.
        
        Returns:
            Optional[pd.DataFrame]: 시간대별 투자자 매매동향 DataFrame
        """
        self._log_info(f"시간대별 투자자 매매동향 수집 시작 ({self.fid_input_iscd_2})")
        
        params = {
            "FID_INPUT_ISCD": self.fid_input_iscd,
            "FID_INPUT_ISCD_2": self.fid_input_iscd_2,
        }

        try:
            response = await self._request_with_retry(self.tr_id, params, method="GET")
            
            if response and response.get('output'):
                df = pd.DataFrame(response['output'])
                df = self._process_dataframe(df)
                self._log_success(f"데이터 수집 완료 ({len(df)} 행)")
                return df
            else:
                self._log_warning("수집된 데이터가 없습니다")
                return None
        except Exception as e:
            self._log_error(f"데이터 수집 중 오류 발생: {e}")
            return None
