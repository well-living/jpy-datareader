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
LIMIT = 100_000

TRANSLATION_MAPPING = {
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
        self.dotenv_path = dotenv_path
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
        Prefer existing environment variables first, then try dotenv files.

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
            "ESTAT_API_KEY",
        ]

        # 1) Prefer already-set environment variables
        for var_name in env_vars:
            api_key = os.getenv(var_name)
            if api_key:
                return api_key

        # 2) Try dotenv files (won't override existing env vars)
        if DOTENV_AVAILABLE:
            if dotenv_path:
                p = Path(dotenv_path)
                if p.exists():
                    load_dotenv(p, override=False)
                    for var_name in env_vars:
                        api_key = os.getenv(var_name)
                        if api_key:
                            return api_key
            else:
                # Try default .env files in this order
                for env_file in [".estat_env", ".env_estat", ".env"]:
                    p = Path(env_file)
                    if p.exists():
                        load_dotenv(p, override=False)
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
            self._store_params_in_attrs(response_data)
            
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
        self._store_params_in_attrs(response_data)

        table_inf_list = response_data.get("DATALIST_INF", {}).get("TABLE_INF", [])

        if isinstance(table_inf_list, dict):
            # If TABLE_INF is a single dictionary, convert it to a list
            table_inf_list = [table_inf_list]
        # Extract and process table information
        table_inf = pd.json_normalize(
            table_inf_list, sep="_"
        )
        
        # Clean column names
        table_inf = table_inf.assign(**{
            col.replace("@", "").rstrip("_$"): table_inf[col] 
            for col in table_inf.columns
        }).drop(columns=table_inf.columns.tolist())

        return table_inf
    
    def _store_params_in_attrs(self, response_data: Dict[str, Any]) -> None:
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
        level_to: Optional[int] = None,
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
        self.level_to = level_to
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
            result_dfs = self.read_class_objs()
            
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

    def read_class_objs(self) -> List[Dict[str, Any]]:
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
            
            # クラス名を取得
            class_name = co.get("@name")
            if not class_name:
                print(f"警告: クラス名（@name）が見つかりません。処理をスキップします。クラスID: {class_id}")
                continue  # このクラスをスキップして次のクラスに進む
            
            class_data = co.get("CLASS")
            
            # 列名変換前の生データフレームを作成
            class_df_raw = self._create_class_dataframe(class_data)
            
            if class_df_raw is None:
                continue

            # Check if hierarchy processing is needed (生データで判定)
            hierarchy_df = None
            if (self.has_lv_hierarchy and 
                "@level" in class_df_raw.columns and 
                len(class_df_raw["@level"].unique()) > 1):
                # Use the method to create hierarchy
                hierarchy_df = self.create_hierarchy_dataframe(json_data, i, level_to=self.level_to)

            # 列名変換を実行
            class_df = self._apply_colname_transformations(class_df_raw, class_name)

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

    def _create_class_dataframe(self, class_data: Union[List[Dict[str, Any]], Dict[str, Any]]) -> Optional[pd.DataFrame]:
        """
        Create raw DataFrame from class data without column transformations.
        
        Parameters
        ----------
        class_data : Union[List[Dict[str, Any]], Dict[str, Any]]
            Class data from API response (can be list or dict)
            
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
            print(f"Error creating raw DataFrame: {e}")
            return None

    def _apply_colname_transformations(self, df: pd.DataFrame, class_name: Optional[str]=None) -> pd.DataFrame:
        """
        Apply column name transformations to DataFrame.
        
        Parameters
        ----------
        df : pd.DataFrame
            Raw DataFrame
        class_name : Optional[str]
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

    def create_class_dataframe(self, class_data: Union[List[Dict[str, Any]], Dict[str, Any]], class_obj: Dict[str, Any]) -> Optional[pd.DataFrame]:
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
        # Get raw dataframe
        raw_df = self._create_class_dataframe(class_data)
        if raw_df is None:
            return None
        
        # クラス名の取得
        class_name = class_obj.get("@name")
        # Apply transformations
        return self._apply_colname_transformations(raw_df, class_name)

    def create_hierarchy_dataframe(self, 
            metainfo: Dict[str, Any], 
            cat_key: int,
            level_to: Optional[int]=None
        ) -> Optional[pd.DataFrame]:
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
        level_to: Optional[int]
            If specified, only columns up to this level will be included.

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
                
                hierarchy_df[level_cols] = hierarchy_df[level_cols].ffill(axis=1)
                hierarchy_df[hierarchy_cols] = hierarchy_df[hierarchy_cols].ffill(axis=1)

            if isinstance(level_to, int) and 0 < level_to < max_level:
                # 指定された最大階層レベルまでの列のみを残す
                hierarchy_df = hierarchy_df[
                    [f"level{lv}" for lv in range(1, level_to + 1)] +
                    [f"{meta_name}階層{lv}" for lv in range(1, level_to + 1)]
                ].drop_duplicates()

            return hierarchy_df
            
        except Exception as e:
            print(f"Error creating hierarchy DataFrame: {e}")
            return None

    def hierarchy_level(self, df: pd.DataFrame, id: str) -> pd.DataFrame:
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
        levels = df[df["level"] == "1"][["code"]]
        levels.columns = ["level1"]
        for lv in np.sort(df["level"].unique().astype(int))[1:]:
            child = df[df["level"] == str(lv)][["parentCode", "code"]]
            child.columns = ["level" + str(lv - 1), "level" + str(lv)]
            levels = levels.merge(child, on="level" + str(lv - 1), how="left")
        if self.use_fillna_lv_hierarchy:
            levels = levels.fillna(method="ffill", axis=1)
        levels.columns = list(
            map(lambda x: id + "_" + x, levels.columns.values.tolist())
        )
        return levels


class StatsDataReader(_eStatReader):
    """
    Reader for e-Stat statistics data API.
    統計データ取得 API
    URL: https://www.e-stat.go.jp/api/api-info/e-stat-manual3-0#api_3_4

    
    Parameters
    ----------
    api_key : str
        e-Stat application ID (appId)
    statsDataId : Union[str, int]
        Statistics data ID
    prefix_colname_with_classname : bool, default True
        Whether to prefix column names with class names
    lang : Optional[str], default None
        Language for retrieved data. Either "J" (Japanese) or "E" (English).
        取得するデータの言語を 以下のいずれかを指定して下さい。
        ・J：日本語 (省略値)
        ・E：英語
    explanationGetFlg : Optional[str], default None
        解説情報有無－統計表及び、提供統計、提供分類、各事項の解説を取得するか否かを以下のいずれかから指定して下さい。
        ・Y：取得する (省略値)
        ・N：取得しない
    metaGetFlg : Optional[str], default None
        メタ情報有無－統計データと一緒にメタ情報を取得するか否かを以下のいずれかから指定して下さい。
        ・Y：取得する (省略値)
        ・N：取得しない
        CSV形式のデータ呼び出しの場合、本パラメータは無効（N：取得しない）です。
    cntGetFlg : Optional[str], default None
        件数取得フラグ－指定した場合、件数のみ取得できます。metaGetFlg=Yの場合は、メタ情報も同時に返却されます。
        ・Y：件数のみ取得する。統計データは取得しない。
        ・N：件数及び統計データを取得する。(省略値)
        CSV形式のデータ呼び出しの場合、本パラメータは無効（N：件数及び統計データを取得する）です。
    annotationGetFlg : Optional[str], default None
        注釈情報有無－数値データの注釈を取得するか否かを以下のいずれかから指定して下さい。
        ・Y：取得する (省略値)
        ・N：取得しない
    replaceSpChar : int, default 2
        特殊文字の置換－特殊文字を置換するか否かを設定します。
        ・置換しない：0（デフォルト）
        ・0（ゼロ）に置換する：1
        ・NULL（長さ0の文字列、空文字)に置換する：2
        ・NA（文字列）に置換する：3
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
        prefix_colname_with_classname: bool = True,
        lang: Optional[str] = None,
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
        # Category 01
        lvCat01: Optional[Union[str, int]] = None,
        cdCat01: Optional[Union[str, int]] = None,
        cdCat01From: Optional[Union[str, int]] = None,
        cdCat01To: Optional[Union[str, int]] = None,
        # Category 02
        lvCat02: Optional[Union[str, int]] = None,
        cdCat02: Optional[Union[str, int]] = None,
        cdCat02From: Optional[Union[str, int]] = None,
        cdCat02To: Optional[Union[str, int]] = None,
        # Category 03
        lvCat03: Optional[Union[str, int]] = None,
        cdCat03: Optional[Union[str, int]] = None,
        cdCat03From: Optional[Union[str, int]] = None,
        cdCat03To: Optional[Union[str, int]] = None,
        # Category 04
        lvCat04: Optional[Union[str, int]] = None,
        cdCat04: Optional[Union[str, int]] = None,
        cdCat04From: Optional[Union[str, int]] = None,
        cdCat04To: Optional[Union[str, int]] = None,
        # Category 05
        lvCat05: Optional[Union[str, int]] = None,
        cdCat05: Optional[Union[str, int]] = None,
        cdCat05From: Optional[Union[str, int]] = None,
        cdCat05To: Optional[Union[str, int]] = None,
        # Category 06
        lvCat06: Optional[Union[str, int]] = None,
        cdCat06: Optional[Union[str, int]] = None,
        cdCat06From: Optional[Union[str, int]] = None,
        cdCat06To: Optional[Union[str, int]] = None,
        # Category 07
        lvCat07: Optional[Union[str, int]] = None,
        cdCat07: Optional[Union[str, int]] = None,
        cdCat07From: Optional[Union[str, int]] = None,
        cdCat07To: Optional[Union[str, int]] = None,
        # Category 08
        lvCat08: Optional[Union[str, int]] = None,
        cdCat08: Optional[Union[str, int]] = None,
        cdCat08From: Optional[Union[str, int]] = None,
        cdCat08To: Optional[Union[str, int]] = None,
        # Category 09
        lvCat09: Optional[Union[str, int]] = None,
        cdCat09: Optional[Union[str, int]] = None,
        cdCat09From: Optional[Union[str, int]] = None,
        cdCat09To: Optional[Union[str, int]] = None,
        # Category 10
        lvCat10: Optional[Union[str, int]] = None,
        cdCat10: Optional[Union[str, int]] = None,
        cdCat10From: Optional[Union[str, int]] = None,
        cdCat10To: Optional[Union[str, int]] = None,
        # Category 11
        lvCat11: Optional[Union[str, int]] = None,
        cdCat11: Optional[Union[str, int]] = None,
        cdCat11From: Optional[Union[str, int]] = None,
        cdCat11To: Optional[Union[str, int]] = None,
        # Category 12
        lvCat12: Optional[Union[str, int]] = None,
        cdCat12: Optional[Union[str, int]] = None,
        cdCat12From: Optional[Union[str, int]] = None,
        cdCat12To: Optional[Union[str, int]] = None,
        # Category 13
        lvCat13: Optional[Union[str, int]] = None,
        cdCat13: Optional[Union[str, int]] = None,
        cdCat13From: Optional[Union[str, int]] = None,
        cdCat13To: Optional[Union[str, int]] = None,
        # Category 14
        lvCat14: Optional[Union[str, int]] = None,
        cdCat14: Optional[Union[str, int]] = None,
        cdCat14From: Optional[Union[str, int]] = None,
        cdCat14To: Optional[Union[str, int]] = None,
        # Category 15
        lvCat15: Optional[Union[str, int]] = None,
        cdCat15: Optional[Union[str, int]] = None,
        cdCat15From: Optional[Union[str, int]] = None,
        cdCat15To: Optional[Union[str, int]] = None,
        # Other parameters
        startPosition: Optional[Union[str, int]] = None,
        limit: int = LIMIT,
        metaGetFlg: Optional[str] = None,
        cntGetFlg: Optional[str] = None,
        annotationGetFlg: Optional[str] = None,
        replaceSpChar: int = 2,
        na_values: Any = np.nan,
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

        if api_key is None:
            raise TypeError("api_key cannot be None")
        # または
        if not isinstance(api_key, str):
            raise TypeError("api_key must be a string")
        self.statsDataId = statsDataId
        self.prefix_colname_with_classname = prefix_colname_with_classname
        
        # Store all filter parameters
        self.filter_params = {
            "lvTab": lvTab, "cdTab": cdTab, "cdTabFrom": cdTabFrom, "cdTabTo": cdTabTo,
            "lvTime": lvTime, "cdTime": cdTime, "cdTimeFrom": cdTimeFrom, "cdTimeTo": cdTimeTo,
            "lvArea": lvArea, "cdArea": cdArea, "cdAreaFrom": cdAreaFrom, "cdAreaTo": cdAreaTo,
            "lvCat01": lvCat01, "cdCat01": cdCat01, "cdCat01From": cdCat01From, "cdCat01To": cdCat01To,
            "lvCat02": lvCat02, "cdCat02": cdCat02, "cdCat02From": cdCat02From, "cdCat02To": cdCat02To,
            "lvCat03": lvCat03, "cdCat03": cdCat03, "cdCat03From": cdCat03From, "cdCat03To": cdCat03To,
            "lvCat04": lvCat04, "cdCat04": cdCat04, "cdCat04From": cdCat04From, "cdCat04To": cdCat04To,
            "lvCat05": lvCat05, "cdCat05": cdCat05, "cdCat05From": cdCat05From, "cdCat05To": cdCat05To,
            "lvCat06": lvCat06, "cdCat06": cdCat06, "cdCat06From": cdCat06From, "cdCat06To": cdCat06To,
            "lvCat07": lvCat07, "cdCat07": cdCat07, "cdCat07From": cdCat07From, "cdCat07To": cdCat07To,
            "lvCat08": lvCat08, "cdCat08": cdCat08, "cdCat08From": cdCat08From, "cdCat08To": cdCat08To,
            "lvCat09": lvCat09, "cdCat09": cdCat09, "cdCat09From": cdCat09From, "cdCat09To": cdCat09To,
            "lvCat10": lvCat10, "cdCat10": cdCat10, "cdCat10From": cdCat10From, "cdCat10To": cdCat10To,
            "lvCat11": lvCat11, "cdCat11": cdCat11, "cdCat11From": cdCat11From, "cdCat11To": cdCat11To,
            "lvCat12": lvCat12, "cdCat12": cdCat12, "cdCat12From": cdCat12From, "cdCat12To": cdCat12To,
            "lvCat13": lvCat13, "cdCat13": cdCat13, "cdCat13From": cdCat13From, "cdCat13To": cdCat13To,
            "lvCat14": lvCat14, "cdCat14": cdCat14, "cdCat14From": cdCat14From, "cdCat14To": cdCat14To,
            "lvCat15": lvCat15, "cdCat15": cdCat15, "cdCat15From": cdCat15From, "cdCat15To": cdCat15To,
        }
        
        self.startPosition = startPosition
        if limit > LIMIT:
            self.limit = LIMIT
            self.max = limit
        else:
            self.limit = limit
            self.max = None
        self.metaGetFlg = metaGetFlg
        self.cntGetFlg = cntGetFlg
        self.annotationGetFlg = annotationGetFlg
        self.replaceSpChar = replaceSpChar
        self.na_values = na_values
        # Initialize metainfo attribute
        self.metainfo = None

    @property
    def url(self) -> str:
        """API URL for getStatsData."""
        return self.get_url("getStatsData")

    @property
    def metainfo_url(self) -> str:
        """API URL for getMetaInfo."""
        return self.get_url("getMetaInfo")

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

    def get_metadata(self) -> Dict[str, Any]:
        """
        Get metadata for the statistics data using MetaInfoReader.
        
        Returns
        -------
        Dict[str, Any]
            Metadata information
        """
        # Create MetaInfoReader instance with same parameters
        metainfo_reader = MetaInfoReader(
            api_key=self.api_key,
            statsDataId=self.statsDataId,
            prefix_colname_with_classname=self.prefix_colname_with_classname,
            lang=self.lang,
            explanationGetFlg=self.explanationGetFlg,
            retry_count=self.retry_count,
            pause=self.pause,
            timeout=self.timeout,
            session=self.session,
            dotenv_path=self.dotenv_path
        )
        
        # Get metadata using MetaInfoReader
        metadata_json = metainfo_reader.read_json()
        
        # Store metadata as instance attribute
        self.metainfo = metadata_json
        
        # Copy important attributes from MetaInfoReader to this instance
        metadata_attributes = [
            "STATUS", "ERROR_MSG", "DATE", "LANG", "DATA_FORMAT", "TABLE_INF",
            "STAT_NAME", "GOV_ORG", "STATISTICS_NAME", "TITLE", "CYCLE",
            "SURVEY_DATE", "OPEN_DATE", "SMALL_AREA", "COLLECT_AREA",
            "MAIN_CATEGORY", "SUB_CATEGORY", "OVERALL_TOTAL_NUMBER",
            "UPDATED_DATE", "STATISTICS_NAME_SPEC", "TABULATION_SUB_CATEGORY1",
            "DESCRIPTION", "TITLE_SPEC"
        ]
        
        for attr in metadata_attributes:
            if hasattr(metainfo_reader, attr):
                setattr(self, attr, getattr(metainfo_reader, attr))
        
        return metadata_json

    def _get_total_number(self) -> int:
        """
        Get total number of records from metadata.
        
        Returns
        -------
        int
            Total number of records
        """
        if not hasattr(self, 'metainfo') or self.metainfo is None:
            self.get_metadata()
        
        return int(self.OVERALL_TOTAL_NUMBER) if hasattr(self, 'OVERALL_TOTAL_NUMBER') and self.OVERALL_TOTAL_NUMBER else 0


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
            self._store_params_in_attrs(json_data)
            
            return json_data
        finally:
            self.close()

    def read(self, split_by_unit: bool = False) -> Union[pd.DataFrame, Dict[str, pd.DataFrame]]:
        """
        Read data from connector.
        
        Parameters
        ----------
        split_by_unit : bool, default False
            Whether to split data by units
            
        Returns
        -------
        Union[pd.DataFrame, Dict[str, pd.DataFrame]]
            Statistics data as DataFrame or dictionary of DataFrames
        """
        try:
            data = self._read(self.url, self.params)
            if split_by_unit:
                return self._split_by_units(data)
            else:
                return data
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
        # Check if data exceeds 100,000 records
        total_number = self._get_total_number()
        
        if total_number > LIMIT:
            return self._read_with_pagination(url, params)
        else:
            return self._read_one_data(url, params)

    def _read_with_pagination(self, url: str, params: Dict[str, Any]) -> pd.DataFrame:
        """
        Read data with automatic pagination for large datasets using NEXT_KEY.
        
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
        dfs = []
        
        try:
            # First data retrieval
            first_data = self._get_response(url, params=params).json()
            first_df = self._process_single_response(first_data)
            
            if not first_df.empty:
                dfs.append(first_df)
            else:
                print("Warning: First data retrieval returned empty DataFrame")
            
            # Continue fetching data using NEXT_KEY
            while "NEXT_KEY" in first_data.get("GET_STATS_DATA", {}).get("STATISTICAL_DATA", {}).get("RESULT_INF", {}):
                try:
                    # Get next position from NEXT_KEY
                    start_position = first_data["GET_STATS_DATA"]["STATISTICAL_DATA"]["RESULT_INF"]["NEXT_KEY"]
                    print("NEXT_KEY: ", start_position)
                    
                    # Break if position exceeds maximum
                    if int(start_position) > self.max:
                        print(f"NEXT_KEY position {start_position} exceeds limit {self.max}. Stopping pagination.")
                        break
                    
                    # Update params with new start position
                    next_params = params.copy()
                    next_params["startPosition"] = start_position
                    
                    # Calculate remaining records and adjust limit if necessary
                    remaining_records = self.max - int(start_position) + 1
                    if remaining_records < self.limit:
                        next_params["limit"] = remaining_records
                        print(f"Adjusting limit to {remaining_records} for remaining records")
                    
                    # Fetch next data
                    next_data = self._get_response(url, params=next_params).json()
                    next_df = self._process_single_response(next_data)
                    
                    if not next_df.empty:
                        dfs.append(next_df)
                        first_data = next_data  # Update for next iteration
                    else:
                        print(f"Warning: Data retrieval at position {start_position} returned empty DataFrame")
                        break
                    
                except KeyError as e:
                    print(f"Error: Missing key in pagination response: {e}")
                    print("Stopping pagination and returning data collected so far.")
                    break
                except Exception as e:
                    print(f"Error during pagination at position {start_position}: {e}")
                    print("Stopping pagination and returning data collected so far.")
                    break
        
        except Exception as e:
            print(f"Error in initial data retrieval: {e}")
            print("Returning empty DataFrame or data collected so far.")
        
        # Combine all DataFrames
        if dfs:
            try:
                result_df = pd.concat(dfs, ignore_index=True)
                print(f"Successfully combined {len(dfs)} DataFrames with total {len(result_df)} rows")
                return result_df
            except Exception as e:
                print(f"Error combining DataFrames: {e}")
                print("Returning first available DataFrame or empty DataFrame")
                return dfs[0] if dfs else pd.DataFrame()
        else:
            print("No data retrieved. Returning empty DataFrame.")
            return pd.DataFrame()

    def _process_single_response(self, response_data: Dict[str, Any]) -> pd.DataFrame:
        """
        Process a single API response and return DataFrame.
        
        Parameters
        ----------
        response_data : Dict[str, Any]
            Single API response data
            
        Returns
        -------
        pd.DataFrame
            Processed DataFrame
        """
        # Store metadata
        self._store_params_in_attrs(response_data)
        
        # Process VALUE data using the new method
        value_df = self._statsjson_to_dataframe(response_data)

        # Handle missing values
        value_df = self._handle_missing_values(value_df, response_data)

        # Process class objects and merge metadata
        value_df = self._merge_class_metadata(value_df, response_data)

        # Apply naming conventions
        value_df = self._apply_colname_transformations(value_df, response_data)


        return value_df

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
        return self._process_single_response(out)

    def _store_params_in_attrs(self, json_data: Dict[str, Any]) -> None:
        """Store statistics metadata as instance attributes."""
        stats_data = json_data.get("GET_STATS_DATA", {})
        
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

    def _statsjson_to_dataframe(self, data: Dict[str, Any]) -> pd.DataFrame:
        """
        Convert statistics JSON data to DataFrame.
        
        Parameters
        ----------
        data : Dict[str, Any]
            JSON response data from e-Stat API
            
        Returns
        -------
        pd.DataFrame
            DataFrame with cleaned column names
            ['tab', 'cat01', 'cat02', 'cat03', 'area', 'time', 'unit', 'value']
        """
        value_df = pd.json_normalize(
            data,
            record_path=["GET_STATS_DATA", "STATISTICAL_DATA", "DATA_INF", "VALUE"]
        ).rename(
            columns=lambda col: col.lstrip("@").replace("$", "value")
        )
        
        # Store attribute list for later use
        self.category_columns = [col for col in value_df.columns if col != "value"]
        
        return value_df
    
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
        # 空のDataFrameの場合は早期リターン
        if value_df.empty:
            return value_df
        
        # value列が存在しない場合は早期リターン
        if 'value' not in value_df.columns:
            return value_df
    
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
                    "value": value_df["value"].replace(note_chars, self.na_values)
                })
        
        # Convert to float if na_values is NaN
        if pd.isna(self.na_values):
            value_df = value_df.assign(**{
                "value": pd.to_numeric(value_df["value"], errors="coerce")
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
            DataFrame with merged metadata (without column name conversion)
            ['tab_code', 'cat01_code', 'cat02_code', 'cat03_code', 'area_code',
            'time_code', 'unit_code', 'value', 'tab_name', 'tab_level',
            'cat01_name', 'cat01_level', 'cat01_unit', 'cat01_parentCode',
            'cat02_name', 'cat02_level', 'cat03_name', 'cat03_level', 'area_name',
            'area_level', 'time_name', 'time_level']
        """
        class_obj = out.get("GET_STATS_DATA", {}) \
                    .get("STATISTICAL_DATA", {}) \
                    .get("CLASS_INF", {}) \
                    .get("CLASS_OBJ", [])

        if not isinstance(class_obj, list):
            print("CLASS_OBJはlist型ではありません。")
            return value_df

        # Add "_code" suffix to attribute columns, but skip if col == "unit"
        new_columns = {}
        for col in value_df.columns:
            if col in self.category_columns and col != "unit":
                new_columns[col] = f"{col}_code"
            else:
                new_columns[col] = col

        value_df = value_df.rename(columns=new_columns)

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

            # Set up merge - only basic column cleaning
            class_df = class_df.set_index("@code")

            # Basic column name cleaning with class-specific prefixes to avoid conflicts
            class_id = co["@id"]
            class_df = class_df.rename(columns=lambda c: f"{class_id}_{c.lstrip('@')}")

            # Merge with value data
            value_df = value_df.merge(
                class_df,
                left_on=f"{class_id}_code",
                right_index=True,
                how="left"
            )

        return value_df

    

    def _apply_colname_transformations(self, value_df: pd.DataFrame, out: Dict[str, Any]) -> pd.DataFrame:

        """
        クラスＩＤ→表示名マップ（CLASS_NAME_MAP）を先に作成し、
        FIELD_MAPPING はそれを使って動的に生成する実装例。
        """
        class_objs = (
            out.get("GET_STATS_DATA", {})
            .get("STATISTICAL_DATA", {})
            .get("CLASS_INF", {})
            .get("CLASS_OBJ", [])
        )
        if not isinstance(class_objs, list):
            return value_df

        # 英語モードの場合は変換をスキップ
        if self.lang == "E":
            # tab_colname設定のみ
            self.tab_colname = "tab_name"
            return value_df
    
        # クラスＩＤ→クラス名マップだけを先に作成
        #    例: {"tab": "表章項目", "cat01": "用途分類", ...}
        self.CLASS_NAME_MAPPING = {
            co["@id"]: co.get("@name", co["@id"])
            for co in class_objs
            if "@id" in co
        }

        # 
        columns_mapping = {}
        for orig in value_df.columns:
            if "_" not in orig:
                continue
            prefix, suffix = orig.split("_", 1)
            # prefix が CLASS_NAME_MAP にある場合だけ変換
            if prefix in self.CLASS_NAME_MAPPING:
                display = self.CLASS_NAME_MAPPING[prefix]
                # 'code','name','level','unit' ... などをそのまま suffix として連結
                columns_mapping[orig] = f"{display}{suffix}"

        # tab_colname 設定（既存ロジック）
        if self.lang == "E":
            self.tab_colname = "tab_name"
        else:
            self.tab_colname = "表章項目"

        # リネーム実行
        if columns_mapping:
            value_df = value_df.rename(columns=columns_mapping)

        # 日本語化ヘルパー適用（lang!="E" のとき）
        if self.lang != "E":
            value_df = colname_to_japanese(value_df)

        return value_df

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
        if "単位" in data.columns:
            unit_col = "単位"
        elif "unit" in data.columns:
            unit_col = "unit"

        # Split by unit
        datasets = {}
        for unit in data[unit_col].unique():
            unit_data = data[data[unit_col] == unit]
            datasets[unit] = unit_data
        
        return datasets

    
def colname_to_japanese(value: pd.DataFrame) -> pd.DataFrame:
    # 英語と日本語の対応
    def _convert(c):
        for k, v in TRANSLATION_MAPPING.items():
            if k in c:
                return c.replace(k, v)
        return c
    return value.rename(columns=_convert)


