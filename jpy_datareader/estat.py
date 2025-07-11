# jpy_datareader/estat.py
# -*- coding: utf-8 -*-

import os
import time
import urllib
import warnings
from typing import Tuple, List, Dict, Any, Optional, Union
from pathlib import Path

import numpy as np
import pandas as pd
import requests

try:
    from dotenv import load_dotenv
    DOTENV_AVAILABLE = True
except ImportError:
    DOTENV_AVAILABLE = False

from jpy_datareader.base import _BaseReader

_version = "3.0"
_BASE_URL = f"https://api.e-stat.go.jp/rest/{_version}/app/json"

ATTR_DICT = {
    "value": "値", 
    "code": "コード", 
    "name": "", 
    "level": "階層レベル", 
    "tab": "表章項目", 
    "cat": "分類", 
    "area": "地域", 
    "time": "時間軸", 
    "unit": "単位", 
    "parentCode": "親コード", 
    "addInf": "追加情報", 
    "annotation": "注釈記号",
}

class _eStatReader(_BaseReader):
    """
    Base class for eStat API readers.

    Parameters
    ----------
    api_key : Optional[str], default None
        取得したアプリケーションIDを指定して下さい。
        eStat API key. If None, will try to get from environment variables
        in the following order:
        E_STAT_APPLICATION_ID, ESTAT_APPLICATION_ID,
        E_STAT_APP_ID, ESTAT_APP_ID,
        E_STAT_APPID, ESTAT_APPID,
        E_STAT_API_KEY, ESTAT_API_KEY
    lang : str, default "J"
        取得するデータの言語を 以下のいずれかを指定して下さい。
        ・J：日本語 (省略値)
        ・E：英語
        Language for retrieved data. Either "J" (Japanese) or "E" (English).
    explanationGetFlg : Optional[str], default None
        統計表及び、提供統計、提供分類、各事項の解説を取得するか否かを以下のいずれかから指定して下さい。
        ・Y：取得する (省略値)
        ・N：取得しない
        Flag for getting explanation data ("Y" or "N").
    retry_count : int, default 3
        Number of times to retry query request.
    pause : float, default 0.1
        Time, in seconds, of the pause between retries.
    timeout : int, default 30
        Request timeout in seconds.
    session : Optional[requests.Session], default None
        requests.sessions.Session instance to be used.
    dotenv_path : Optional[str], default None
        Path to .env file for loading environment variables.
        If None, will look for .estat_env, .env_estat, or .env in the current directory.
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        lang: Optional[str] = None,
        explanationGetFlg: Optional[str] = None,
        retry_count: int = 3,
        pause: float = 0.1,
        timeout: int = 30,
        session: Optional[requests.Session] = None,
        dotenv_path: Optional[str] = None,
    ) -> None:
        super().__init__(
            retry_count=retry_count,
            pause=pause,
            timeout=timeout,
            session=session,
        )

        # Try to get API key from various sources
        if api_key is None:
            api_key = self._get_api_key_from_env(dotenv_path)
                
        if not api_key or not isinstance(api_key, str):
            raise ValueError(
                "The e-Stat Application ID must be provided either "
                "through the api_key variable or through one of the "
                "following environment variables: "
                "E_STAT_APPLICATION_ID, ESTAT_APPLICATION_ID, "
                "E_STAT_APP_ID, ESTAT_APP_ID, "
                "E_STAT_APPID, ESTAT_APPID, "
                "E_STAT_API_KEY, ESTAT_API_KEY"
            )

        self.api_key = api_key
        self.explanationGetFlg = explanationGetFlg
        self.lang = lang

    def _get_api_key_from_env(self, dotenv_path: Optional[str] = None) -> Optional[str]:
        """
        Get API key from environment variables or .env files.
        First tries dotenv files, then falls back to environment variables.
        
        Parameters
        ----------
        dotenv_path : Optional[str]
            Path to specific .env file to load. If None, tries default files.
            
        Returns
        -------
        Optional[str]
            API key if found, None otherwise
        """
        # Environment variable names to try in order
        env_vars = [
            "E_STAT_APPLICATION_ID",
            "ESTAT_APPLICATION_ID", 
            "E_STAT_APP_ID",
            "ESTAT_APP_ID",
            "E_STAT_APPID",
            "ESTAT_APPID",
            "E_STAT_API_KEY",
            "ESTAT_API_KEY"
        ]
        
        # First try dotenv files if available
        if DOTENV_AVAILABLE:
            if dotenv_path:
                # If specific dotenv path is provided
                if Path(dotenv_path).exists():
                    load_dotenv(dotenv_path)
                    # Try all environment variables after loading the specified file
                    for var_name in env_vars:
                        api_key = os.getenv(var_name)
                        if api_key:
                            return api_key
            else:
                # Try default .env files
                env_files = [".estat_env", ".env_estat", ".env"]
                
                for env_file in env_files:
                    if Path(env_file).exists():
                        load_dotenv(env_file)
                        # Try all environment variables after loading each file
                        for var_name in env_vars:
                            api_key = os.getenv(var_name)
                            if api_key:
                                return api_key
        
        # Fallback to regular environment variables
        for var_name in env_vars:
            api_key = os.getenv(var_name)
            if api_key:
                return api_key
                
        return None

    def get_url(self, path: str = "getStatsData") -> str:
        """
        Get API URL for specified path.
        
        Parameters
        ----------
        path : str, default "getStatsList"
            API endpoint path
            
        Returns
        -------
        str
            Complete API URL
        """
        valid_paths = ["getStatsList", "getDataCatalog", "getMetaInfo", "getStatsData"]
        if path not in valid_paths:
            path = "getStatsData"
            print(
                f"pathは{', '.join(valid_paths)}で指定します。pathをgetStatsDataに置換しました。"
            )
        return f"{_BASE_URL}/{path}?"


class StatsListReader(_eStatReader):
    """
    Reader for eStat statistics list API.
    統計表情報取得 API
    URL: https://www.e-stat.go.jp/api/api-info/e-stat-manual3-0#api_3_2
    
    Parameters
    ----------
    api_key : str
        e-Stat application ID (appId)
    lang : Optional[str], default None
        Language for retrieved data. Either "J" (Japanese) or "E" (English).
        取得するデータの言語を 以下のいずれかを指定して下さい。
        ・J：日本語 (省略値)
        ・E：英語
    explanationGetFlg : Optional[str], default None
        Flag for getting explanation data ("Y" or "N")
        統計表及び、提供統計、提供分類の解説を取得するか否かを指定
        ・Y：取得する (省略値)
        ・N：取得しない
    retry_count : int, default 3
        Number of times to retry query request
    pause : float, default 0.1
        Time, in seconds, of the pause between retries
    timeout : int, default 300
        Request timeout in seconds
    session : Optional[requests.Session], default None
        requests.sessions.Session instance to be used
    dotenv_path : Optional[str], default None
        Path to .env file for loading environment variables.
        If None, will look for .estat_env, .env_estat, or .env in the current directory.
    surveyYears : Optional[Union[str, int]], default None
        Survey years filter
        調査年月
        以下のいずれかの形式で指定
        ・yyyy：単年検索
        ・yyyymm：単月検索
        ・yyyymm-yyyymm：範囲検索
    openYears : Optional[Union[str, int]], default None
        Open years filter
        公開年月
        調査年月と同様の形式で指定
        ・yyyy：単年検索
        ・yyyymm：単月検索
        ・yyyymm-yyyymm：範囲検索
    statsField : Optional[Union[str, int]], default None
        Statistics field filter
        統計分野
        以下のいずれかの形式で指定
        ・数値2桁：統計大分類で検索
        ・数値4桁：統計小分類で検索
    statsCode : Optional[Union[str, int]], default None
        Statistics code filter
        政府統計コード
        以下のいずれかの形式で指定
        ・数値5桁：作成機関で検索
        ・数値8桁：政府統計コードで検索
    searchWord : Optional[str], default None
        Search word filter
        検索キーワード
        任意の文字列。表題やメタ情報等に含まれている文字列を検索
        AND、OR 又は NOT を指定して複数ワードでの検索が可能
        (東京 AND 人口、東京 OR 大阪 等)
    searchKind : Optional[int], default None
        Search kind (1 or 2)
        検索データ種別
        検索するデータの種別を指定
        ・1：統計情報(省略値)
        ・2：小地域・地域メッシュ
    collectArea : Optional[int], default None
        Collection area (1-3)
        集計地域区分
        検索するデータの集計地域区分を指定
        ・1：全国
        ・2：都道府県
        ・3：市区町村
    statsNameList : Optional[str], default None
        Statistics name list flag ("Y")
        統計調査名指定
        統計表情報でなく、統計調査名の一覧を取得する場合に指定
        ・Y：統計調査名一覧
        統計調査名一覧を出力。省略時又はY以外の値を設定した場合は統計表情報を出力
    startPosition : Optional[Union[str, int]], default None
        Start position for pagination
        データ取得開始位置
        データの取得開始位置（1から始まる行番号）を指定。省略時は先頭から取得
        統計データを複数回に分けて取得する場合等、継続データを取得する開始位置を指定
        前回受信したデータの<NEXT_KEY>タグの値を指定
    limit : Optional[Union[str, int]], default None
        Limit for pagination
        データ取得件数
        データの取得行数を指定。省略時は10万件
        データ件数が指定したlimit値より少ない場合、全件を取得
        データ件数が指定したlimit値より多い場合（継続データが存在する）は、
        受信したデータの<NEXT_KEY>タグに継続データの開始行が設定
    updatedDate : Optional[Union[str, int]], default None
        Updated date filter
        更新日付
        更新日付を指定。指定された期間で更新された統計表の情報を提供
        以下のいずれかの形式で指定
        ・yyyy：単年検索
        ・yyyymm：単月検索
        ・yyyymmdd：単日検索
        ・yyyymmdd-yyyymmdd：範囲検索
    """
    def __init__(
        self,
        api_key: str,
        lang: Optional[str] = None,
        explanationGetFlg: Optional[str] = None,
        retry_count: int = 3,
        pause: float = 0.1,
        timeout: int = 300,
        session: Optional[requests.Session] = None,
        dotenv_path: Optional[str] = None,
        surveyYears: Optional[Union[str, int]] = None,
        openYears: Optional[Union[str, int]] = None,
        statsField: Optional[Union[str, int]] = None,
        statsCode: Optional[Union[str, int]] = None,
        searchWord: Optional[str] = None,
        searchKind: Optional[int] = None,
        collectArea: Optional[int] = None,
        statsNameList: Optional[str] = None,
        startPosition: Optional[Union[str, int]] = None,
        limit: Optional[Union[str, int]] = None,
        updatedDate: Optional[Union[str, int]] = None,
    ) -> None:
        super().__init__(
            api_key=api_key,
            lang=lang,
            explanationGetFlg=explanationGetFlg,
            retry_count=retry_count,
            pause=pause,
            timeout=timeout,
            session=session,
            dotenv_path=dotenv_path,
        )

        self.surveyYears = surveyYears
        self.openYears = openYears
        self.statsField = statsField
        self.statsCode = statsCode
        self.searchWord = searchWord
        self.searchKind = searchKind
        self.collectArea = collectArea
        self.statsNameList = statsNameList
        self.startPosition = startPosition
        self.limit = limit
        self.updatedDate = updatedDate

    @property
    def url(self) -> str:
        """API URL for getStatsList."""
        return self.get_url("getStatsList")

    @property
    def params(self) -> Dict[str, Any]:
        """Parameters to use in API calls."""
        pdict = {"appId": self.api_key}

        # Add parameters if they are set and valid
        param_mappings = [
            ("surveyYears", self.surveyYears, lambda x: isinstance(x, (str, int))),
            ("openYears", self.openYears, lambda x: isinstance(x, (str, int))),
            ("statsField", self.statsField, lambda x: isinstance(x, (str, int))),
            ("statsCode", self.statsCode, lambda x: isinstance(x, (str, int))),
            ("searchWord", self.searchWord, lambda x: isinstance(x, str)),
            ("searchKind", self.searchKind, lambda x: x in [1, 2]),
            ("collectArea", self.collectArea, lambda x: x in range(1, 4)),
            ("explanationGetFlg", self.explanationGetFlg, lambda x: x in ["Y", "N"]),
            ("statsNameList", self.statsNameList, lambda x: x == "Y"),
            ("startPosition", self.startPosition, lambda x: isinstance(x, (str, int))),
            ("limit", self.limit, lambda x: isinstance(x, (str, int))),
            ("updatedDate", self.updatedDate, lambda x: isinstance(x, (str, int))),
        ]

        for param_name, param_value, validator in param_mappings:
            if param_value is not None and validator(param_value):
                pdict[param_name] = param_value

        return pdict

    def read(self):
        """Read data from connector"""
        try:
            return self._read_one_data(self.url, self.params)
        finally:
            self.close()

    def read_json(self) -> Dict[str, Any]:
        """Read data from connector and return as raw JSON."""
        try:
            response = self._get_response(self.url, params=self.params)
            json_data = response.json()
            
            # Store response metadata as instance attributes
            response_data = json_data.get("GET_STATS_LIST", {})
            self._store_response_metadata(response_data)
            
            return json_data
        finally:
            self.close()

    def _read_one_data(self, url: str, params: Dict[str, Any]) -> pd.DataFrame:
        """
        Read one data from specified URL.
        
        Parameters
        ----------
        url : str
            Target URL
        params : Dict[str, Any]
            Request parameters
            
        Returns
        -------
        pd.DataFrame
            Processed statistics list data
        """
        out = self._get_response(url, params=params).json()

        # Store response metadata as instance attributes
        response_data = out.get("GET_STATS_LIST", {})
        self._store_response_metadata(response_data)

        # Extract and process table information
        table_inf = pd.json_normalize(
            out, record_path=["GET_STATS_LIST", "DATALIST_INF", "TABLE_INF"], sep="_"
        )
        
        # Clean column names
        table_inf = table_inf.assign(**{
            col.replace("@", "").rstrip("_$"): table_inf[col] 
            for col in table_inf.columns
        }).drop(columns=table_inf.columns.tolist())

        return table_inf
    
    def _store_response_metadata(self, response_data: Dict[str, Any]) -> None:
        """Store response metadata as instance attributes."""
        # Store RESULT metadata
        result = response_data.get("RESULT", {})
        self.STATUS = result.get("STATUS")
        self.ERROR_MSG = result.get("ERROR_MSG")
        self.DATE = result.get("DATE")

        # Store PARAMETER metadata
        parameter = response_data.get("PARAMETER", {})
        self.LANG = parameter.get("LANG")
        self.DATA_FORMAT = parameter.get("DATA_FORMAT")
        self.LIMIT = parameter.get("LIMIT")

        # Store DATALIST_INF metadata
        datalist_inf = response_data.get("DATALIST_INF", {})
        self.NUMBER = datalist_inf.get("NUMBER")
        
        result_inf = datalist_inf.get("RESULT_INF", {})
        self.FROM_NUMBER = result_inf.get("FROM_NUMBER")
        self.TO_NUMBER = result_inf.get("TO_NUMBER")
        self.NEXT_KEY = result_inf.get("NEXT_KEY")


class MetaInfoReader(_eStatReader):
    """
    Reader for e-Stat meta infomation API.
    メタ情報取得 API
    URL: https://www.e-stat.go.jp/api/api-info/e-stat-manual3-0#api_3_3
    
    Parameters
    ----------
    api_key : str
        e-Stat application ID (appId)
    statsDataId : Union[str, int]
        Statistics data ID
        「統計表情報取得」で得られる統計表IDです。
    prefix_colname_with_classname: bool, default True
        Whether to prefix column names with class names
    has_lv_hierarchy : bool, default False
        Whether to create hierarchy levels
    use_fillna_lv_hierarchy : bool, default False
        Whether to fill NA values in hierarchy levels
    lang : Optional[str], default None
        Language for retrieved data. Either "J" (Japanese) or "E" (English).
        取得するデータの言語を 以下のいずれかを指定して下さい。
        ・J：日本語 (省略値)
        ・E：英語
    explanationGetFlg : Optional[str], default None
        Flag for getting explanation data ("Y" or "N")
        統計表及び、提供統計、提供分類、各事項の解説を取得するか否かを以下のいずれかから指定して下さい。
        ・Y：取得する (省略値)
        ・N：取得しない
    retry_count : int, default 3
        Number of times to retry query request
    pause : float, default 0.1
        Time, in seconds, of the pause between retries
    timeout : int, default 30
        Request timeout in seconds
    session : Optional[requests.Session], default None
        requests.sessions.Session instance to be used
    dotenv_path : Optional[str], default None
        Path to .env file for loading environment variables.
        If None, will look for .estat_env, .env_estat, or .env in the current directory.
    """

    def __init__(
        self,
        api_key: str,
        statsDataId: Union[str, int],
        prefix_colname_with_classname: bool = True,
        has_lv_hierarchy: bool = False,
        use_fillna_lv_hierarchy: bool = True,
        lang: Optional[str] = None,
        explanationGetFlg: Optional[str] = None,
        retry_count: int = 3,
        pause: float = 0.1,
        timeout: int = 30,
        session: Optional[requests.Session] = None,
        dotenv_path: Optional[str] = None,
    ) -> None:
        super().__init__(
            api_key=api_key,
            lang=lang,
            explanationGetFlg=explanationGetFlg,
            retry_count=retry_count,
            pause=pause,
            timeout=timeout,
            session=session,
            dotenv_path=dotenv_path,
        )

        self.statsDataId = statsDataId
        self.prefix_colname_with_classname = prefix_colname_with_classname
        self.has_lv_hierarchy = has_lv_hierarchy
        self.use_fillna_lv_hierarchy = use_fillna_lv_hierarchy

    @property
    def url(self) -> str:
        """API URL for getMetaInfo."""
        return self.get_url("getMetaInfo")

    @property
    def params(self) -> Dict[str, Any]:
        """Parameters to use in API calls."""
        pdict = {"appId": self.api_key}

        if isinstance(self.statsDataId, (str, int)):
            pdict["statsDataId"] = self.statsDataId
        if self.explanationGetFlg in ["Y", "N"]:
            pdict["explanationGetFlg"] = self.explanationGetFlg

        return pdict
    
    def read(self) -> pd.DataFrame:
        """
        Read data from connector and return the DataFrame with the most rows.
        Excludes DataFrames with 'id': 'time'.
        
        Returns
        -------
        pd.DataFrame
            DataFrame with the most rows from all CLASS_OBJ DataFrames (excluding 'time')
        """
        try:
            result_dfs = self.read_class_obj_dfs()
            
            if not result_dfs:
                return pd.DataFrame()
            
            # Find the DataFrame with the most rows (excluding 'time')
            max_rows = 0
            largest_df = pd.DataFrame()
            
            for class_data in result_dfs:
                # Skip if id is 'time'
                if class_data["id"] == 'time':
                    continue
                
                df = class_data["meta_dataframe"]
                
                if len(df) > max_rows:
                    max_rows = len(df)
                    largest_df = df
            
            return largest_df
            
        finally:
            self.close()

    def read_json(self) -> Dict[str, Any]:
        """Read data from connector and return as raw JSON."""
        try:
            response = self._get_response(self.url, params=self.params)
            json_data = response.json()
            
            # Store response metadata as instance attributes
            self._store_params_in_attrs(json_data)
            
            return json_data 
        finally:
            self.close()

    def read_class_obj_dfs(self) -> List[Dict[str, Any]]:
        """
        Read and process CLASS_OBJ data into DataFrames.
        CLASS_OBJ dictionary's keys: ['@id', '@name', 'CLASS']
        CLASS dictionary's keys: ['@code', '@name', '@level', '@unit']
        
        Returns
        -------
        List[Dict[str, Any]]
            List of dictionaries with keys: "id", "name", "meta_dataframe", "hierarchy"
        """
        response = self._get_response(self.url, params=self.params)
        json_data = response.json()
        
        # Store response metadata as instance attributes
        self._store_params_in_attrs(json_data)
        
        # Get class objects
        meta_info = json_data.get("GET_META_INFO", {})
        class_obj = meta_info.get("METADATA_INF", {}).get("CLASS_INF", {}).get("CLASS_OBJ", [])
        
        if not isinstance(class_obj, list):
            print("CLASS_OBJはlist型ではありません。")
            return []
        
        result_dfs = []
        
        for i, co in enumerate(class_obj):
            # クラスIDを取得
            class_id = co.get("@id")
            if not class_id:
                print(f"警告: クラスID（@id）が見つかりません。処理をスキップします。")
                continue
            
            # クラス名を取得（複数の方法で試行）
            class_name = co.get("@name")
            if not class_name:
                print(f"警告: クラス名（@name）が見つかりません。処理をスキップします。クラスID: {class_id}")
                continue  # このクラスをスキップして次のクラスに進む
            
            class_data = co.get("CLASS")
            
            # 列名変換前の生データフレームを作成
            class_df_raw = self._create_class_dataframe_raw(class_data, co)
            
            if class_df_raw is None:
                continue

            # Check if hierarchy processing is needed (生データで判定)
            hierarchy_df = None
            if (self.has_lv_hierarchy and 
                "@level" in class_df_raw.columns and 
                len(class_df_raw["@level"].unique()) > 1):
                # Use the method to create hierarchy
                hierarchy_df = self._create_hierarchy_dataframe(json_data, i)

            # 列名変換を実行
            class_df = self._apply_column_transformations(class_df_raw, class_name)

            # Create result dictionary
            result_dict = {
                "id": class_id,
                "name": class_name,
                "meta_dataframe": class_df,
            }

            if hierarchy_df is not None:
                result_dict["hierarchy"] = hierarchy_df
            
            result_dfs.append(result_dict)
        
        return result_dfs

    def _store_params_in_attrs(self, json_data: Dict[str, Any]) -> None:
        """Store params in attributes as instance variables."""
        # GET_META_INFOセクションを取得
        meta_info = json_data.get("GET_META_INFO", {})
        
        # RESULTセクションの処理
        result = meta_info.get("RESULT", {})
        self.STATUS = result.get("STATUS")
        self.ERROR_MSG = result.get("ERROR_MSG")
        self.DATE = result.get("DATE")

        # PARAMETERセクションの処理
        parameter = meta_info.get("PARAMETER", {})
        self.LANG = parameter.get("LANG")
        self.DATA_FORMAT = parameter.get("DATA_FORMAT")

        # METADATA_INFセクションの取得
        metadata_inf = meta_info.get("METADATA_INF", {})
        
        # TABLE_INFセクションの処理
        table_inf = metadata_inf.get("TABLE_INF", {})
        self.TABLE_INF = table_inf
        
        # Store individual table attributes
        table_attributes = [
            "STAT_NAME", "GOV_ORG", "STATISTICS_NAME", "TITLE", "CYCLE",
            "SURVEY_DATE", "OPEN_DATE", "SMALL_AREA", "COLLECT_AREA",
            "MAIN_CATEGORY", "SUB_CATEGORY", "OVERALL_TOTAL_NUMBER",
            "UPDATED_DATE", "STATISTICS_NAME_SPEC", "TABULATION_SUB_CATEGORY1",
            "DESCRIPTION", "TITLE_SPEC"
        ]
        
        for attr in table_attributes:
            setattr(self, attr, table_inf.get(attr))

    def _create_class_dataframe_raw(self, class_data: Union[List[Dict[str, Any]], Dict[str, Any]], class_obj: Dict[str, Any]) -> Optional[pd.DataFrame]:
        """
        Create raw DataFrame from class data without column transformations.
        
        Parameters
        ----------
        class_data : Union[List[Dict[str, Any]], Dict[str, Any]]
            Class data from API response (can be list or dict)
        class_obj : Dict[str, Any]
            Class object metadata
            
        Returns
        -------
        Optional[pd.DataFrame]
            Raw DataFrame created from class data, or None if failed
        """
        if not class_data:
            return None
            
        try:
            # Handle different types of class_data
            if isinstance(class_data, list):
                df = pd.DataFrame(class_data)
            elif isinstance(class_data, dict):
                df = pd.DataFrame(pd.Series(class_data)).T
            else:
                print(f"CLASS_INF>CLASS_OBJ>CLASSの型: {type(class_data)}")
                return None
            
            # Convert level to int if exists, handle empty strings
            if "@level" in df.columns:
                # Replace empty strings with NaN, then convert to nullable int
                df = df.assign(**{
                    "@level": lambda d: pd.to_numeric(d["@level"].replace("", pd.NA), errors="coerce").astype("Int64")
                })

            return df
            
        except Exception as e:
            print(f"Error creating raw DataFrame for class {class_obj.get('@id', 'unknown')}: {e}")
            return None

    def _apply_column_transformations(self, df: pd.DataFrame, class_name: str) -> pd.DataFrame:
        """
        Apply column name transformations to DataFrame.
        
        Parameters
        ----------
        df : pd.DataFrame
            Raw DataFrame
        class_name : str
            Class name for prefixing
            
        Returns
        -------
        pd.DataFrame
            DataFrame with transformed column names
        """
        # Create a copy to avoid modifying the original
        transformed_df = df.copy()
        
        # Rename columns with class name prefix
        if self.prefix_colname_with_classname:
            # クラス名をプレフィックスとして付加
            transformed_df = transformed_df.rename(columns=lambda col: f"{class_name}{col.lstrip('@')}")
        else:
            # プレフィックスなし、@記号のみ除去
            transformed_df = transformed_df.rename(columns=lambda col: f"{col.lstrip('@')}")

        if self.lang is None or self.lang != "E":
            # Convert column names to Japanese
            transformed_df = colname_to_japanese(transformed_df).rename(columns={"": class_name})

        return transformed_df

    def _create_class_dataframe(self, class_data: Union[List[Dict[str, Any]], Dict[str, Any]], class_obj: Dict[str, Any]) -> Optional[pd.DataFrame]:
        """
        Create DataFrame from class data with full transformations.
        
        Parameters
        ----------
        class_data : Union[List[Dict[str, Any]], Dict[str, Any]]
            Class data from API response (can be list or dict)
        class_obj : Dict[str, Any]
            Class object metadata
            
        Returns
        -------
        Optional[pd.DataFrame]
            DataFrame created from class data, or None if failed
        """
        # クラス名の取得（@name → @code の順で試行）
        class_name = class_obj.get("@name")
        if not class_name:
            print(f"警告: クラス名（@name）が見つかりません。処理をスキップします。クラスID: {class_obj.get('@id', 'unknown')}")
            return None
        
        # Get raw dataframe
        raw_df = self._create_class_dataframe_raw(class_data, class_obj)
        if raw_df is None:
            return None
        
        # Apply transformations
        return self._apply_column_transformations(raw_df, class_name)

    def _create_hierarchy_dataframe(self, metainfo: Dict[str, Any], cat_key: int) -> Optional[pd.DataFrame]:
        """
        Create a hierarchical DataFrame based on metadata information.
        
        This method creates a DataFrame where each row represents a bottom-level node
        in the hierarchy, with columns for each hierarchical level containing 
        "code_name" format values. Missing intermediate levels are forward-filled.

        Parameters
        ----------
        metainfo : Dict[str, Any]
            Metadata information containing hierarchical data with @code, @name, 
            @level, and @parentCode fields
        cat_key : int
            Target category key index

        Returns
        -------
        Optional[pd.DataFrame]
            Hierarchical DataFrame with bottom-level nodes as rows and 
            hierarchical levels as columns, or None if failed
        """
        try:
            # Extract target category metadata
            class_obj_list = metainfo["GET_META_INFO"]["METADATA_INF"]["CLASS_INF"]["CLASS_OBJ"]
            
            if cat_key >= len(class_obj_list):
                print(f"警告: cat_key {cat_key} が範囲外です。")
                return None
                
            cat_meta = class_obj_list[cat_key]
            meta_name = cat_meta["@name"]
            
            class_data = cat_meta.get("CLASS")
            if not class_data:
                print(f"警告: CLASS データが見つかりません。")
                return None
            
            # Handle different types of class_data
            if isinstance(class_data, list):
                meta_cls_df = pd.DataFrame(class_data)
            elif isinstance(class_data, dict):
                meta_cls_df = pd.DataFrame(pd.Series(class_data)).T
            else:
                print(f"CLASS データの型が不正です: {type(class_data)}")
                return None
            
            # Convert level to int
            if "@level" not in meta_cls_df.columns:
                print(f"警告: @level 列が見つかりません。")
                return None
                
            meta_cls_df = meta_cls_df.assign(
                **{"@level": lambda df: pd.to_numeric(df["@level"], errors="coerce").astype("Int64")}
            )
            
            # Create set of parent codes for identifying leaf nodes
            parent_codes = {
                row.get("@parentCode") 
                for _, row in meta_cls_df.iterrows() 
                if row.get("@parentCode") and str(row.get("@parentCode")).strip()
            }
            
            # Create code-to-record mapping
            code_to_record = {row["@code"]: row for _, row in meta_cls_df.iterrows()}

            def _get_ancestry_chain(meta_record: Dict[str, Any]) -> Dict[int, str]:
                """
                Get ancestry chain for a metadata record.
                
                Parameters
                ----------
                meta_record : Dict[str, Any]
                    Metadata record with @code, @name, @level, @parentCode fields

                Returns
                -------
                Dict[int, str]
                    Dictionary mapping level to code for the ancestry chain
                """
                chain = {}
                current_record = meta_record
                
                while current_record is not None:
                    level = current_record["@level"]
                    chain[level] = current_record["@code"]
                    parent_code = current_record.get("@parentCode")
                    
                    if not parent_code or parent_code not in code_to_record:
                        break
                        
                    current_record = code_to_record[parent_code]
                
                return chain

            # Process leaf nodes only
            max_level = meta_cls_df["@level"].max()
            chain_rows = []
            
            for _, row in meta_cls_df.iterrows():
                # Skip parent nodes
                if row["@code"] in parent_codes:
                    continue

                node_level = row["@level"]
                ancestry = _get_ancestry_chain(row)
                row_chain = {}
                last_code = None
                
                # Build hierarchy with forward fill
                for level in range(1, max_level + 1):
                    col = f"level{level}"
                    if level <= node_level:
                        if level in ancestry:
                            last_code = ancestry[level]
                            row_chain[col] = ancestry[level]
                        else:
                            row_chain[col] = last_code if self.use_fillna_lv_hierarchy else None
                    else:
                        row_chain[col] = None
                        
                chain_rows.append(row_chain)

            if not chain_rows:
                print(f"警告: 階層データが生成されませんでした。")
                return None

            hierarchy_df = pd.DataFrame(chain_rows)

            # Merge with names to create "code_name" format
            for level in range(1, max_level + 1):
                level_col = f"level{level}"
                name_col = f"{meta_name}階層{level}"
                
                name_df = meta_cls_df[["@code", "@name"]].assign(
                    **{
                        level_col: meta_cls_df["@code"],
                        name_col: meta_cls_df["@code"] + "_" + meta_cls_df["@name"]
                    }
                )[["@code", name_col]].rename(columns={"@code": level_col})
                
                hierarchy_df = hierarchy_df.merge(name_df, on=level_col, how="left")

            # Apply forward fill to both code and name columns if enabled
            if self.use_fillna_lv_hierarchy:
                level_cols = [f"level{level}" for level in range(1, max_level + 1)]
                hierarchy_cols = [f"{meta_name}階層{level}" for level in range(1, max_level + 1)]
                
                hierarchy_df[level_cols] = hierarchy_df[level_cols].fillna(method="ffill", axis=1)
                hierarchy_df[hierarchy_cols] = hierarchy_df[hierarchy_cols].fillna(method="ffill", axis=1)

            return hierarchy_df
            
        except Exception as e:
            print(f"Error creating hierarchy DataFrame: {e}")
            return None

    def hierarchy_level(self, df: pd.DataFrame, class_id: str) -> pd.DataFrame:
        """
        Create hierarchy level DataFrame.
        
        .. deprecated:: 
            This method is deprecated. Use the external create_hierarchy_dataframe function instead.
        
        Parameters
        ----------
        df : pd.DataFrame
            Class DataFrame with hierarchy information
        class_id : str
            Class ID for column naming
            
        Returns
        -------
        pd.DataFrame
            Hierarchy levels DataFrame
        """
        warnings.warn(
            "hierarchy_level method is deprecated. Use the external create_hierarchy_dataframe function instead.",
            DeprecationWarning,
            stacklevel=2
        )
        # This method is kept for backward compatibility but should not be used
        return pd.DataFrame()



class StatsDataReader(_eStatReader):
    """
    Reader for eStat statistics data API.
    統計データ取得 API
    URL: https://www.e-stat.go.jp/api/api-info/e-stat-manual3-0#api_3_4

    
    Parameters
    ----------
    api_key : str
        e-Stat application ID (appId)
    statsDataId : Union[str, int]
        Statistics data ID
    name_or_id : str, default "name"
        Whether to use "name" or "id" for column naming
    explanationGetFlg : Optional[str], default None
        Flag for getting explanation data ("Y" or "N")
    retry_count : int, default 3
        Number of times to retry query request
    pause : float, default 0.1
        Time, in seconds, of the pause between retries
    timeout : int, default 30
        Request timeout in seconds
    session : Optional[requests.Session], default None
        requests.sessions.Session instance to be used
    dotenv_path : Optional[str], default None
        Path to .env file for loading environment variables.
    Various filter parameters for data selection (lvTab, cdTab, etc.)
    limit : int, default 100000
        Limit for pagination
    na_values : Any, default np.nan
        Value to use for missing data
    """

    def __init__(
        self,
        api_key: str,
        statsDataId: Union[str, int],
        name_or_id: str = "name",
        explanationGetFlg: Optional[str] = None,
        retry_count: int = 3,
        pause: float = 0.1,
        timeout: int = 30,
        session: Optional[requests.Session] = None,
        dotenv_path: Optional[str] = None,
        # Filter parameters
        lvTab: Optional[Union[str, int]] = None,
        cdTab: Optional[Union[str, int]] = None,
        cdTabFrom: Optional[Union[str, int]] = None,
        cdTabTo: Optional[Union[str, int]] = None,
        lvTime: Optional[Union[str, int]] = None,
        cdTime: Optional[Union[str, int]] = None,
        cdTimeFrom: Optional[Union[str, int]] = None,
        cdTimeTo: Optional[Union[str, int]] = None,
        lvArea: Optional[Union[str, int]] = None,
        cdArea: Optional[Union[str, int]] = None,
        cdAreaFrom: Optional[Union[str, int]] = None,
        cdAreaTo: Optional[Union[str, int]] = None,
        lvCat01: Optional[Union[str, int]] = None,
        cdCat01: Optional[Union[str, int]] = None,
        cdCat01From: Optional[Union[str, int]] = None,
        cdCat01To: Optional[Union[str, int]] = None,
        lvCat02: Optional[Union[str, int]] = None,
        cdCat02: Optional[Union[str, int]] = None,
        cdCat02From: Optional[Union[str, int]] = None,
        cdCat02To: Optional[Union[str, int]] = None,
        lvCat03: Optional[Union[str, int]] = None,
        cdCat03: Optional[Union[str, int]] = None,
        cdCat03From: Optional[Union[str, int]] = None,
        cdCat03To: Optional[Union[str, int]] = None,
        startPosition: Optional[Union[str, int]] = None,
        limit: int = 100000,
        metaGetFlg: Optional[str] = None,
        cntGetFlg: Optional[str] = None,
        annotationGetFlg: Optional[str] = None,
        replaceSpChar: int = 2,
        na_values: Any = np.nan,
    ) -> None:
        super().__init__(
            api_key=api_key,
            explanationGetFlg=explanationGetFlg,
            retry_count=retry_count,
            pause=pause,
            timeout=timeout,
            session=session,
            dotenv_path=dotenv_path,
        )

        self.statsDataId = statsDataId
        self.name_or_id = name_or_id
        
        # Store all filter parameters
        self.filter_params = {
            "lvTab": lvTab, "cdTab": cdTab, "cdTabFrom": cdTabFrom, "cdTabTo": cdTabTo,
            "lvTime": lvTime, "cdTime": cdTime, "cdTimeFrom": cdTimeFrom, "cdTimeTo": cdTimeTo,
            "lvArea": lvArea, "cdArea": cdArea, "cdAreaFrom": cdAreaFrom, "cdAreaTo": cdAreaTo,
            "lvCat01": lvCat01, "cdCat01": cdCat01, "cdCat01From": cdCat01From, "cdCat01To": cdCat01To,
            "lvCat02": lvCat02, "cdCat02": cdCat02, "cdCat02From": cdCat02From, "cdCat02To": cdCat02To,
            "lvCat03": lvCat03, "cdCat03": cdCat03, "cdCat03From": cdCat03From, "cdCat03To": cdCat03To,
        }
        
        self.startPosition = startPosition
        self.limit = limit
        self.metaGetFlg = metaGetFlg
        self.cntGetFlg = cntGetFlg
        self.annotationGetFlg = annotationGetFlg
        self.replaceSpChar = replaceSpChar
        self.na_values = na_values

    @property
    def url(self) -> str:
        """API URL for getStatsData."""
        return self.get_url("getStatsData")

    @property
    def params(self) -> Dict[str, Any]:
        """Parameters to use in API calls."""
        pdict = {"appId": self.api_key}

        # Add statsDataId
        if isinstance(self.statsDataId, (str, int)):
            pdict["statsDataId"] = self.statsDataId

        # Add filter parameters
        for param_name, param_value in self.filter_params.items():
            if isinstance(param_value, (str, int)):
                pdict[param_name] = param_value

        # Add other parameters
        if isinstance(self.startPosition, (str, int)):
            pdict["startPosition"] = self.startPosition
        if isinstance(self.limit, (str, int)):
            pdict["limit"] = self.limit
        if self.metaGetFlg in ["Y", "N"]:
            pdict["metaGetFlg"] = self.metaGetFlg
        if self.cntGetFlg in ["Y", "N"]:
            pdict["cntGetFlg"] = self.cntGetFlg
        if self.explanationGetFlg in ["Y", "N"]:
            pdict["explanationGetFlg"] = self.explanationGetFlg
        if self.annotationGetFlg in ["Y", "N"]:
            pdict["annotationGetFlg"] = self.annotationGetFlg
        if self.replaceSpChar in range(4):
            pdict["replaceSpChar"] = self.replaceSpChar

        return pdict

    def read(self, normal: bool = True, split_units: bool = False) -> Union[pd.DataFrame, Dict[str, pd.DataFrame]]:
        """
        Read data from connector.
        
        Parameters
        ----------
        normal : bool, default True
            Whether to return normalized data
        split_units : bool, default False
            Whether to split data by units
            
        Returns
        -------
        Union[pd.DataFrame, Dict[str, pd.DataFrame]]
            Statistics data as DataFrame or dictionary of DataFrames
        """
        try:
            data = self._read(self.url, self.params)
            if normal:
                return data
            else:
                if split_units:
                    return self._split_by_units(data)
                else:
                    return self._denormalize_data(data)
        finally:
            self.close()

    def read_json(self) -> Dict[str, Any]:
        """
        Read data from connector and return as raw JSON.
        
        Returns
        -------
        Dict[str, Any]
            Raw JSON response from the API
        """
        try:
            response = self._get_response(self.url, params=self.params)
            json_data = response.json()
            
            # Store response metadata as instance attributes
            stats_data = json_data.get("GET_STATS_DATA", {})
            self._store_stats_metadata(stats_data)
            
            return json_data
        finally:
            self.close()

    def _read(self, url: str, params: Dict[str, Any]) -> pd.DataFrame:
        """
        Read data with automatic pagination handling.
        
        Parameters
        ----------
        url : str
            Target URL
        params : Dict[str, Any]
            Request parameters
            
        Returns
        -------
        pd.DataFrame
            Combined statistics data
        """
        if self.limit is None:
            return self._read_with_pagination(url, params)
        else:
            return self._read_one_data(url, params)

    def _read_with_pagination(self, url: str, params: Dict[str, Any]) -> pd.DataFrame:
        """
        Read data with automatic pagination for large datasets.
        
        Parameters
        ----------
        url : str
            Target URL
        params : Dict[str, Any]
            Request parameters
            
        Returns
        -------
        pd.DataFrame
            Combined statistics data
        """
        # First, get total count
        test_params = {**params, "limit": 1}
        out = self._get_response(url, params=test_params).json()
        total_number = out["GET_STATS_DATA"]["STATISTICAL_DATA"]["TABLE_INF"]["OVERALL_TOTAL_NUMBER"]

        if total_number <= 100000:
            return self._read_one_data(url, params)

        # For large datasets, split by classification
        return self._read_with_classification_split(url, params, out, total_number)

    def _read_with_classification_split(
        self, 
        url: str, 
        params: Dict[str, Any], 
        sample_response: Dict[str, Any],
        total_number: int
    ) -> pd.DataFrame:
        """
        Read large datasets by splitting on classification dimensions.
        
        Parameters
        ----------
        url : str
            Target URL
        params : Dict[str, Any]
            Request parameters
        sample_response : Dict[str, Any]
            Sample response for determining split strategy
        total_number : int
            Total number of records
            
        Returns
        -------
        pd.DataFrame
            Combined statistics data
        """
        ptrans = {
            "tab": "cdTab", "time": "cdTime", "area": "cdArea",
            "cat01": "cdCat01", "cat02": "cdCat02", "cat03": "cdCat03"
        }

        # Determine split strategy based on classification sizes
        class_obj = sample_response["GET_STATS_DATA"]["STATISTICAL_DATA"]["CLASS_INF"]["CLASS_OBJ"]
        cls_info = []
        
        for n, co in enumerate(class_obj):
            if isinstance(co.get("CLASS"), list):
                cls_info.append([n, co["@id"], len(co["CLASS"])])
        
        cls_df = pd.DataFrame(cls_info, columns=["index", "id", "size"]).sort_values("size", ascending=False)

        # Calculate split parameters
        param_names = []
        codes = []
        remaining_total = total_number
        
        for _, row in cls_df.iterrows():
            param_name = ptrans.get(row["id"])
            if param_name:
                param_names.append(param_name)
                class_codes = [
                    entry["@code"] for entry in 
                    class_obj[row["index"]]["CLASS"]
                ]
                codes.append(class_codes)
                remaining_total //= row["size"]
                if remaining_total < 100000:
                    break

        # Generate parameter combinations
        codes_product = self._generate_parameter_combinations(codes)
        
        # Fetch data for each combination
        dfs = []
        for combination in codes_product:
            split_params = params.copy()
            for i, param_name in enumerate(param_names):
                split_params[param_name] = combination[i]
            
            try:
                df = self._read_one_data(url, split_params)
                dfs.append(df)
            except Exception as e:
                print(f"Error reading data for combination {combination}: {e}")
                continue

        return pd.concat(dfs, axis=0, ignore_index=True) if dfs else pd.DataFrame()

    def _generate_parameter_combinations(self, codes: List[List[str]]) -> List[Tuple[str, ...]]:
        """
        Generate all combinations of parameter codes.
        
        Parameters
        ----------
        codes : List[List[str]]
            List of code lists for each parameter
            
        Returns
        -------
        List[Tuple[str, ...]]
            List of parameter combinations
        """
        if not codes:
            return []
        
        result = [[]]
        for code_list in codes:
            result = [
                existing + [code] 
                for existing in result 
                for code in code_list
            ]
        
        return [tuple(combination) for combination in result]

    def _read_one_data(self, url: str, params: Dict[str, Any]) -> pd.DataFrame:
        """
        Read one data from specified URL.
        
        Parameters
        ----------
        url : str
            Target URL
        params : Dict[str, Any]
            Request parameters
            
        Returns
        -------
        pd.DataFrame
            Processed statistics data
        """
        out = self._get_response(url, params=params).json()

        # Store response metadata
        self._store_stats_metadata(out)

        # Process VALUE data
        value_data = out["GET_STATS_DATA"]["STATISTICAL_DATA"]["DATA_INF"]["VALUE"]
        value_df = pd.DataFrame(value_data)
        
        # Clean column names
        self.attrlist = [col.lstrip("@") for col in value_df.columns]
        value_df.columns = self.attrlist

        # Handle missing values
        value_df = self._handle_missing_values(value_df, out)

        # Process class objects and merge metadata
        value_df = self._merge_class_metadata(value_df, out)

        # Apply naming conventions
        value_df = self._apply_naming_conventions(value_df)

        return value_df


    def _store_stats_metadata(self, out: Dict[str, Any]) -> None:
        """Store statistics metadata as instance attributes."""
        stats_data = out.get("GET_STATS_DATA", {})
        
        # Store result metadata
        result = stats_data.get("RESULT", {})
        self.STATUS = result.get("STATUS")
        self.ERROR_MSG = result.get("ERROR_MSG")
        self.DATE = result.get("DATE")

        # Store parameter metadata
        parameter = stats_data.get("PARAMETER", {})
        self.LANG = parameter.get("LANG")
        self.DATA_FORMAT = parameter.get("DATA_FORMAT")
        self.START_POSITION = parameter.get("START_POSITION")
        self.METAGET_FLG = parameter.get("METAGET_FLG")

        # Store statistical data metadata
        statistical_data = stats_data.get("STATISTICAL_DATA", {})
        result_inf = statistical_data.get("RESULT_INF", {})
        self.TOTAL_NUMBER = result_inf.get("TOTAL_NUMBER")
        self.FROM_NUMBER = result_inf.get("FROM_NUMBER")
        self.TO_NUMBER = result_inf.get("TO_NUMBER")

        # Store table information
        table_inf = statistical_data.get("TABLE_INF", {})
        self.TABLE_INF = table_inf
        self.STATISTICS_NAME = table_inf.get("STATISTICS_NAME")
        self.CYCLE = table_inf.get("CYCLE")
        self.OVERALL_TOTAL_NUMBER = table_inf.get("OVERALL_TOTAL_NUMBER")

        # Process title and create stats data name
        title = table_inf.get("TITLE", "")
        if isinstance(title, dict):
            self.TITLE = title.get("$", "")
        else:
            self.TITLE = title

        gov_org = table_inf.get("GOV_ORG", {})
        self.GOV_ORG = gov_org.get("$", "") if isinstance(gov_org, dict) else str(gov_org)

        # Create standardized stats data name
        cycle = self.CYCLE if self.CYCLE != "-" else ""
        name_parts = [self.STATISTICS_NAME, self.TITLE, cycle, self.GOV_ORG]
        self.StatsDataName = "_".join(filter(None, name_parts)).replace(" ", "_")

    def _handle_missing_values(self, value_df: pd.DataFrame, out: Dict[str, Any]) -> pd.DataFrame:
        """
        Handle missing values in the data.
        
        Parameters
        ----------
        value_df : pd.DataFrame
            Value DataFrame
        out : Dict[str, Any]
            Full API response
            
        Returns
        -------
        pd.DataFrame
            DataFrame with missing values handled
        """
        note = out.get("GET_STATS_DATA", {}).get("STATISTICAL_DATA", {}).get("DATA_INF", {}).get("NOTE")
        
        if note:
            if isinstance(note, list):
                note_chars = [n["@char"] for n in note]
            elif isinstance(note, dict):
                note_chars = [note["@char"]]
            else:
                note_chars = []
            
            if note_chars:
                value_df = value_df.assign(**{
                    "$": value_df["$"].replace(note_chars, self.na_values)
                })
        
        # Convert to float if na_values is NaN
        if pd.isna(self.na_values):
            value_df = value_df.assign(**{
                "$": pd.to_numeric(value_df["$"], errors="coerce")
            })
            
        return value_df

    def _merge_class_metadata(self, value_df: pd.DataFrame, out: Dict[str, Any]) -> pd.DataFrame:
        """
        Merge class metadata with value data.
        
        Parameters
        ----------
        value_df : pd.DataFrame
            Value DataFrame
        out : Dict[str, Any]
            Full API response
            
        Returns
        -------
        pd.DataFrame
            DataFrame with merged metadata
        """
        class_obj = out.get("GET_STATS_DATA", {}).get("STATISTICAL_DATA", {}).get("CLASS_INF", {}).get("CLASS_OBJ", [])
        
        if not isinstance(class_obj, list):
            print("CLASS_OBJはlist型ではありません。")
            return value_df

        for co in class_obj:
            class_data = co.get("CLASS")
            if not class_data:
                continue
                
            # Convert class data to DataFrame
            if isinstance(class_data, list):
                class_df = pd.DataFrame(class_data)
            elif isinstance(class_data, dict):
                class_df = pd.DataFrame(pd.Series(class_data)).T
            else:
                continue

            # Set up merge
            class_df = class_df.set_index("@code")
            
            # Apply naming convention
            if self.name_or_id == "id":
                class_df = class_df.assign(**{
                    f"{co['@id']}_{col.lstrip('@')}": class_df[col]
                    for col in class_df.columns
                }).drop(columns=class_df.columns.tolist())
                self.tabcol = "tab_name"
            else:
                class_df = class_df.assign(**{
                    f"{co['@name']}{col.lstrip('@')}": class_df[col]
                    for col in class_df.columns
                }).drop(columns=class_df.columns.tolist())
                self.tabcol = "表章項目名"

            # Merge with value data
            value_df = value_df.merge(
                class_df, 
                left_on=co["@id"], 
                right_index=True, 
                how="left"
            )

        return value_df

    def _apply_naming_conventions(self, value_df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply naming conventions to the DataFrame.
        
        Parameters
        ----------
        value_df : pd.DataFrame
            Input DataFrame
            
        Returns
        -------
        pd.DataFrame
            DataFrame with proper naming conventions
        """
        # Add "_code" suffix to attribute columns
        new_columns = {}
        for col in value_df.columns:
            if col in self.attrlist:
                new_columns[col] = f"{col}_code"
            else:
                new_columns[col] = col
        
        value_df = value_df.rename(columns=new_columns)
        value_df = value_df.rename(columns={"$_code": "value"})

        # Apply Japanese naming if requested
        if self.name_or_id == "name":
            value_df = colname_to_japanese(value_df)
            value_df = value_df.rename(columns={"value": "値"})

        return value_df

    def _denormalize_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Denormalize data by removing code/level columns and unstacking.
        
        Parameters
        ----------
        data : pd.DataFrame
            Input normalized data
            
        Returns
        -------
        pd.DataFrame
            Denormalized data
        """
        # Define columns to drop based on naming convention
        if self.name_or_id == "name":
            drop_patterns = ["コード", "階層レベル", "unit", "単位", "親コード", "追加情報"]
        else:
            drop_patterns = ["code", "level", "unit", "parentCode", "addInf"]
        
        # Drop unnecessary columns
        cols_to_drop = [
            col for col in data.columns 
            if any(pattern in col for pattern in drop_patterns)
        ]
        df = data.drop(columns=cols_to_drop)

        # Unstack if tabcol is in index
        if hasattr(self, 'tabcol') and self.tabcol in df.index.names:
            if self.name_or_id == "name":
                name_cols = [col for col in df.columns if "名" in col]
            else:
                name_cols = [col for col in df.columns if "name" in col]
            
            if name_cols:
                df = df.set_index(name_cols)
                df = df.unstack(self.tabcol)
                df.columns = [col[1] for col in df.columns]
                df = df.reset_index()

        # Clean column names
        df = df.assign(**{
            col.replace("_name", "").rstrip("名"): df[col]
            for col in df.columns
        }).drop(columns=df.columns.tolist())

        return df

    def _split_by_units(self, data: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """
        Split data by units.
        
        Parameters
        ----------
        data : pd.DataFrame
            Input data
            
        Returns
        -------
        Dict[str, pd.DataFrame]
            Dictionary of DataFrames split by unit
        """
        # Handle unit columns
        unit_col = None
        if "単位コード" in data.columns:
            unit_col = "単位コード"
        elif "unit_code" in data.columns:
            unit_col = "unit_code"
        
        if unit_col:
            data = data.assign(**{
                "unit": data[unit_col].fillna("単位なし")
            }).drop(columns=[unit_col])
        else:
            data = data.assign(unit="単位なし")

        # Split by unit
        datasets = {}
        for unit in data["unit"].unique():
            unit_data = data[data["unit"] == unit]
            datasets[unit] = self._denormalize_data(unit_data)
        
        return datasets

class DataCatalogReader(_eStatReader):
    """
    Reader for eStat data catalog API.
    データカタログ情報取得 API
    URL+ https://www.e-stat.go.jp/api/api-info/e-stat-manual3-0#api_3_7

    
    Parameters
    ----------
    api_key : str
        e-Stat application ID (appId)
    explanationGetFlg : Optional[str], default None
        Flag for getting explanation data ("Y" or "N")
    retry_count : int, default 3
        Number of times to retry query request
    pause : float, default 0.1
        Time, in seconds, of the pause between retries
    timeout : int, default 30
        Request timeout in seconds
    session : Optional[requests.Session], default None
        requests.sessions.Session instance to be used
    dotenv_path : Optional[str], default None
        Path to .env file for loading environment variables.
    Various filter parameters for catalog search
    limit : int, default 100000
        Limit for pagination
    """

    def __init__(
        self,
        api_key: str,
        explanationGetFlg: Optional[str] = None,
        retry_count: int = 3,
        pause: float = 0.1,
        timeout: int = 30,
        session: Optional[requests.Session] = None,
        dotenv_path: Optional[str] = None,
        surveyYears: Optional[Union[str, int]] = None,
        openYears: Optional[Union[str, int]] = None,
        statsField: Optional[Union[str, int]] = None,
        statsCode: Optional[Union[str, int]] = None,
        searchWord: Optional[str] = None,
        collectArea: Optional[int] = None,
        dataType: Optional[str] = None,
        startPosition: Optional[Union[str, int]] = None,
        catalogId: Optional[Union[str, int]] = None,
        resourceId: Optional[Union[str, int]] = None,
        limit: int = 100000,
        updatedDate: Optional[Union[str, int]] = None,
    ) -> None:
        super().__init__(
            api_key=api_key,
            explanationGetFlg=explanationGetFlg,
            retry_count=retry_count,
            pause=pause,
            timeout=timeout,
            session=session,
            dotenv_path=dotenv_path,
        )

        self.surveyYears = surveyYears
        self.openYears = openYears
        self.statsField = statsField
        self.statsCode = statsCode
        self.searchWord = searchWord
        self.collectArea = collectArea
        self.dataType = dataType
        self.startPosition = startPosition
        self.catalogId = catalogId
        self.resourceId = resourceId
        self.limit = limit
        self.updatedDate = updatedDate

    @property
    def url(self) -> str:
        """API URL for getDataCatalog."""
        return self.get_url("getDataCatalog")

    @property
    def params(self) -> Dict[str, Any]:
        """Parameters to use in API calls."""
        pdict = {"appId": self.api_key}

        # Add parameters with validation
        param_mappings = [
            ("surveyYears", self.surveyYears, lambda x: isinstance(x, (str, int))),
            ("openYears", self.openYears, lambda x: isinstance(x, (str, int))),
            ("statsField", self.statsField, lambda x: isinstance(x, (str, int))),
            ("statsCode", self.statsCode, lambda x: isinstance(x, (str, int))),
            ("searchWord", self.searchWord, lambda x: isinstance(x, str)),
            ("collectArea", self.collectArea, lambda x: x in range(1, 4)),
            ("explanationGetFlg", self.explanationGetFlg, lambda x: x in ["Y", "N"]),
            ("dataType", self.dataType, lambda x: x in ["XLS", "CSV", "PDF", "XML", "XLS_REP", "DB"]),
            ("startPosition", self.startPosition, lambda x: isinstance(x, (str, int))),
            ("catalogId", self.catalogId, lambda x: isinstance(x, (str, int))),
            ("resourceId", self.resourceId, lambda x: isinstance(x, (str, int))),
            ("updatedDate", self.updatedDate, lambda x: isinstance(x, (str, int))),
        ]

        for param_name, param_value, validator in param_mappings:
            if param_value is not None and validator(param_value):
                pdict[param_name] = param_value

        return pdict

    def _read_one_data(self, url: str, params: Dict[str, Any]) -> pd.DataFrame:
        """
        Read one data from specified URL.
        
        Parameters
        ----------
        url : str
            Target URL
        params : Dict[str, Any]
            Request parameters
            
        Returns
        -------
        pd.DataFrame
            Processed data catalog information
        """
        out = self._get_response(url, params=params).json()

        # Store response metadata
        self._store_catalog_metadata(out)

        # Extract and process data catalog information
        data_catalog_inf = pd.json_normalize(
            out,
            record_path=["GET_DATA_CATALOG", "DATA_CATALOG_LIST_INF", "DATA_CATALOG_INF"],
            sep="_",
        )
        
        # Clean column names
        data_catalog_inf = data_catalog_inf.assign(**{
            col.replace("@", "").rstrip("_$"): data_catalog_inf[col]
            for col in data_catalog_inf.columns
        }).drop(columns=data_catalog_inf.columns.tolist())

        return data_catalog_inf

    def _store_catalog_metadata(self, out: Dict[str, Any]) -> None:
        """Store catalog metadata as instance attributes."""
        catalog_data = out.get("GET_DATA_CATALOG", {})
        
        # Store result metadata
        result = catalog_data.get("RESULT", {})
        self.STATUS = result.get("STATUS")
        self.ERROR_MSG = result.get("ERROR_MSG")
        self.DATE = result.get("DATE")

        # Store parameter metadata
        parameter = catalog_data.get("PARAMETER", {})
        self.LANG = parameter.get("LANG")
        self.DATA_FORMAT = parameter.get("DATA_FORMAT")

        # Store catalog list metadata
        catalog_list_inf = catalog_data.get("DATA_CATALOG_LIST_INF", {})
        self.NUMBER = catalog_list_inf.get("NUMBER")
        
        result_inf = catalog_list_inf.get("RESULT_INF", {})
        self.FROM_NUMBER = result_inf.get("FROM_NUMBER")
        self.TO_NUMBER = result_inf.get("TO_NUMBER")
        self.NEXT_KEY = result_inf.get("NEXT_KEY")



def colname_to_japanese(value: pd.DataFrame) -> pd.DataFrame:
    # 英語と日本語の対応
    attrdict = {"value": "値", "code": "コード", "name": "", "level": "階層レベル", 
        "unit": "単位", "parentCode": "親コード", "addInf": "追加情報", "tab": "表章項目", 
        "cat": "分類", "area": "地域", "time": "時間軸", "annotation": "注釈記号"  
    }
    def _convert(c):
        for k, v in attrdict.items():
            if k in c:
                return c.replace(k, v)
        return c
    return value.rename(columns=_convert)







# メタ情報を取得する関数
def get_metainfo(
        appId: str,
        statsDataId: str,
        version: str="3.0",
        timeout: int=10,
    ) -> Dict[str, Any]:
    meta_url = f"https://api.e-stat.go.jp/rest/{version}/app/json/getMetaInfo"
    meta_params = {"appId": appId, "statsDataId": statsDataId}
    try:
        meta_res = requests.get(meta_url, params=meta_params, timeout=timeout)
        meta_res.raise_for_status()
        return meta_res.json()
    except requests.exceptions.HTTPError as e:
        print("HTTPError:", e)
        return {"error": "HTTP request failed"}
    except Exception as e:
        print(f"Exception Error: {e}")
        return {"error": "An unexpected error occurred"}

# 統計データを取得する関数
def get_statsdata(
        appId: str, 
        statsDataId: str, 
        params: Dict[str, Any]=None, 
        version: str="3.0", 
        timeout: int=60
    ) -> Dict[str, Any]:
    if params is None:
        params = {}
    data_url = f"https://api.e-stat.go.jp/rest/{version}/app/json/getStatsData"
    data_params = {"appId": appId, "statsDataId": statsDataId}
    data_params.update(params)
    try:
        data_res = requests.get(data_url, params=data_params, timeout=timeout)
        data_res.raise_for_status()
        return data_res.json()
    except requests.exceptions.HTTPError as e:
        print("HTTPError:", e)
        return {"error": "HTTP request failed"}
    except Exception as e:
        print(f"Exception Error: {e}")
        return {"error": "An unexpected error occurred"}

# 統計データのJSONからDataFrameを抽出する関数
def statsjson_to_dataframe(data: Dict[str, Any]) -> pd.DataFrame:
    value = pd.json_normalize(
        data, 
        record_path=["GET_STATS_DATA", "STATISTICAL_DATA", "DATA_INF", "VALUE"]
    ).rename(
        columns=lambda col: col.lstrip("@").replace("$", "value")
    )
    return value

# 統計データの欠測値をNumPyのNaNに置換する関数
def missing_to_nan(
        value: pd.DataFrame, 
        note: Union[Dict[str, str], List[Dict[str, str]]]
    ) -> pd.DataFrame:
    if isinstance(note, dict):
        note_char = note["@char"]
    elif isinstance(note, list):
        note_char = [n["@char"] for n in note]
    else:
        print(f"引数noteの型は辞書またはリストにしてください。noteの型: {type(note)}")
        return value
    return value.assign(**{
        "value": lambda df: df["value"]
            .replace(note_char, np.nan)
            .astype(float)
    })

# statsjson_to_dataframeとmissing_to_nan、及びメタデータ結合を一括処理する関数
def cleansing_statsdata(data: Dict[str, Any]) -> pd.DataFrame:
    value = statsjson_to_dataframe(data)
    note = data["GET_STATS_DATA"]["STATISTICAL_DATA"]["DATA_INF"].get("NOTE")
    if note:
        value = missing_to_nan(value, note)
    else:
        value = value.assign(**{
            "value": lambda df: df["value"].astype(float)
        })
    class_obj = data["GET_STATS_DATA"]["STATISTICAL_DATA"]["CLASS_INF"]["CLASS_OBJ"]
    for co in class_obj:
        class_entries = co["CLASS"]
        if isinstance(class_entries, list):
            cls_df = pd.DataFrame(class_entries)
        elif isinstance(class_entries, dict):
            cls_df = pd.DataFrame(pd.Series(class_entries)).T
        else:
            print("CLASS_INF>CLASS_OBJ>CLASSの型: ", type(class_entries))
            continue
        cls_df = (cls_df
            .set_index("@code")
            .rename(columns=lambda col: f"{co['@name']}{col.lstrip('@')}")
        )
        value = (value
            .merge(cls_df, left_on=co["@id"], right_index=True, how="left")
            .rename(columns={co["@id"]: f"{co['@name']}code"})
        )
    return value
