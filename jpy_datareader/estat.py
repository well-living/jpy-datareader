# -*- coding: utf-8 -*-

import os
import time

import warnings

import urllib
import requests

import numpy as np
import pandas as pd

from jpy_datareader.base import _BaseReader


_version = "3.0"
_BASE_URL = f"https://api.e-stat.go.jp/rest/{_version}/app/json"

class _eStatReader(_BaseReader):
    """
    Get data for the given name from eStat.

    .. versionadded:: 3.0

    Parameters
    ----------
    api_key : str, optional
        eStat API key.
        取得したアプリケーションID(appId)を指定.
    """

    def __init__(
        self,
        retry_count=3,
        pause=0.1,
        timeout=30,
        session=None,
        api_key=None,
        explanationGetFlg=None,
    ):

        super().__init__(
            retry_count=retry_count,
            pause=pause,
            timeout=timeout,
            session=session,
        )

        if api_key is None:
            api_key = os.getenv("ESTAT_API_KEY")
        if not api_key or not isinstance(api_key, str):
            raise ValueError(
                "The eStat API key must be provided either "
                "through the api_key variable or through the "
                "environmental variable ESTAT_API_KEY."
            )

        self.api_key = api_key
        self.explanationGetFlg = explanationGetFlg

    @property
    def url(self, path="getStatsList"):
        """API URL"""
        if path not in ["getStatsList", "getDataCatalog", "getMetaInfo", "getStatsData"]:
            path = "getStatsList"
            print("pathはgetStatsList, getDataCatalog, getMetaInfo, getStatsDataで指定します。pathをgetStatsListに置換しました。")
        eStat_URL = _BASE_URL + "/{path}?"
        return eStat_URL


    @property
    def params(self):
        """Parameters to use in API calls"""
        pdict = {
            "appId": self.sapi_key,
        }
    
        params = {
            "api_key": self.api_key,
        }
        paramstring = urllib.parse.urlencode(query=params)
        url = "{url}{params}".format(url=StatsList_URL, params=paramstring)
        return url


class StatsListReader(_eStatReader):

    def __init__(
        self,
        api_key,
        explanationGetFlg=None,
        retry_count=3,
        pause=0.1,
        timeout=300,
        session=None,
        surveyYears=None, 
        openYears=None, 
        statsField=None, 
        statsCode=None, 
        searchWord=None, 
        searchKind=None, 
        collectArea=None, 
        statsNameList=None, 
        startPosition=None, 
        limit=None, 
        updatedDate=None, 
    ):

        super().__init__(
            api_key=api_key,
            explanationGetFlg=explanationGetFlg,
            retry_count=retry_count,
            pause=pause,
            timeout=timeout,
            session=session,
        )

        if api_key is None:
            api_key = os.getenv("ESTAT_API_KEY")
        if not api_key or not isinstance(api_key, str):
            raise ValueError(
                "The eStat API key must be provided either "
                "through the api_key variable or through the "
                "environmental variable ESTAT_API_KEY."
            )

        self.api_key = api_key
        self.surveyYears = surveyYears
        self.openYears = openYears
        self.statsField = statsField
        self.statsCode = statsCode
        self.searchWord = searchWord
        self.searchKind = searchKind
        self.collectArea = collectArea
        self.explanationGetFlg = explanationGetFlg
        self.statsNameList = statsNameList
        self.startPosition = startPosition
        self.limit = limit
        self.updatedDate = updatedDate

    @property
    def url(self):
        """API URL"""
        StatsList_URL = _BASE_URL + "/getStatsList?"
        return StatsList_URL

    @property
    def params(self):
        """Parameters to use in API calls"""
        pdict = {
            "appId": self.api_key,
        }
        
        if isinstance(self.surveyYears, (str, int)):
            pdict.update({"surveyYears": self.surveyYears})
        if isinstance(self.surveyYears, (str, int)):
            pdict.update({"surveyYears": self.surveyYears})
        if isinstance(self.openYears, (str, int)):
            pdict.update({"openYears": self.openYears})
        if isinstance(self.statsField, (str, int)):
            pdict.update({"statsField": self.statsField})
        if isinstance(self.statsCode, (str, int)):
            pdict.update({"statsCode": self.statsCode})
        if isinstance(self.searchWord, str):
            pdict.update({"searchWord": self.searchWord})
        if self.searchKind in [1, 2]:
            pdict.update({"searchKind": self.searchKind})
        if self.collectArea in range(1, 4):
            pdict.update({"collectArea": self.collectArea})
        if self.explanationGetFlg in ["Y", "N"]:
            pdict.update({"explanationGetFlg": self.explanationGetFlg})
        if self.statsNameList == "Y":
            pdict.update({"statsNameList": self.statsNameList})
        if isinstance(self.startPosition, (str, int)):
            pdict.update({"startPosition": self.startPosition})
        if isinstance(self.limit, (str, int)):
            pdict.update({"limit": self.limit})
        if isinstance(self.updatedDate, (str, int)):
            pdict.update({"updatedDate": self.updatedDate})
        
        return pdict

    def read(self):
        """Read data from connector"""
        try:
            return self._read_one_data(self.url, self.params)
        finally:
            self.close()

    def _read_one_data(self, url, params):
        """read one data from specified URL"""
        out = self._get_response(url, params=params).json()
        
        self.STATUS = out["GET_STATS_LIST"]["RESULT"]["STATUS"]
        self.ERROR_MSG = out["GET_STATS_LIST"]["RESULT"]["ERROR_MSG"]
        self.DATE = out["GET_STATS_LIST"]["RESULT"]["DATE"]
        self.LANG = out["GET_STATS_LIST"]["PARAMETER"]["LANG"]
        self.DATA_FORMAT = out["GET_STATS_LIST"]["PARAMETER"]["DATA_FORMAT"]
        if "LIMIT" in out["GET_STATS_LIST"]["PARAMETER"].keys():
            self.LIMIT = out["GET_STATS_LIST"]["PARAMETER"]["LIMIT"]
        self.NUMBER = out["GET_STATS_LIST"]["DATALIST_INF"]["NUMBER"]
        self.FROM_NUMBER = out["GET_STATS_LIST"]["DATALIST_INF"]["RESULT_INF"]["FROM_NUMBER"]
        self.TO_NUMBER = out["GET_STATS_LIST"]["DATALIST_INF"]["RESULT_INF"]["TO_NUMBER"]
        if "NEXT_KEY" in out["GET_STATS_LIST"]["DATALIST_INF"]["RESULT_INF"].keys():
            self.NEXT_KEY = out["GET_STATS_LIST"]["DATALIST_INF"]["RESULT_INF"]["NEXT_KEY"]

        TABLE_INF = pd.json_normalize(out, record_path=["GET_STATS_LIST", "DATALIST_INF", "TABLE_INF"], sep="_")
        TABLE_INF.columns = list(map(lambda x: x.replace("@", "").rstrip("_$"), TABLE_INF.columns.values.tolist()))

        return TABLE_INF


class MetaInfoReader(_eStatReader):

    def __init__(
        self,
        api_key,
        explanationGetFlg=None,
        retry_count=3,
        pause=0.1,
        timeout=30,
        session=None,
        statsDataId="",
    ):

        super().__init__(
            api_key=api_key,
            explanationGetFlg=explanationGetFlg,
            retry_count=retry_count,
            pause=pause,
            timeout=timeout,
            session=session,
        )

        if api_key is None:
            api_key = os.getenv("ESTAT_API_KEY")
        if not api_key or not isinstance(api_key, str):
            raise ValueError(
                "The eStat API key must be provided either "
                "through the api_key variable or through the "
                "environmental variable ESTAT_API_KEY."
            )

        self.api_key = api_key
        self.statsDataId = statsDataId
        self.explanationGetFlg = explanationGetFlg

    @property
    def url(self):
        """API URL"""
        MetaInfo_URL = _BASE_URL + "/getMetaInfo?"
        return MetaInfo_URL

    @property
    def params(self):
        """Parameters to use in API calls"""
        pdict = {
            "appId": self.api_key,
        }
        
        if isinstance(self.statsDataId, (str, int)):
            pdict.update({"statsDataId": self.statsDataId})
        if self.explanationGetFlg in ["Y", "N"]:
            pdict.update({"explanationGetFlg": self.explanationGetFlg})

        return pdict

    def read(self):
        """Read data from connector"""
        try:
            return self._read_one_data(self.url, self.params)
        finally:
            self.close()

    def _read_one_data(self, url, params):
        """read one data from specified URL"""
        out = self._get_response(url, params=params).json()

        self.STATUS = out["GET_META_INFO"]["RESULT"]["STATUS"]
        self.ERROR_MSG = out["GET_META_INFO"]["RESULT"]["ERROR_MSG"]
        self.DATE = out["GET_META_INFO"]["RESULT"]["DATE"]
        self.LANG = out["GET_META_INFO"]["PARAMETER"]["LANG"]
        self.DATA_FORMAT = out["GET_META_INFO"]["PARAMETER"]["DATA_FORMAT"]
        self.TABLE_INF = out["GET_META_INFO"]["METADATA_INF"]["TABLE_INF"]
        
        CLASS_OBJ = out["GET_META_INFO"]["METADATA_INF"]["CLASS_INF"]["CLASS_OBJ"]
        dfs = []
        if isinstance(CLASS_OBJ, list):
            for co in CLASS_OBJ:
                CLASS = co["CLASS"]
                if isinstance(CLASS, list):
                    CLASS = pd.DataFrame(co["CLASS"])
                elif isinstance(CLASS, dict):
                    CLASS = pd.DataFrame(pd.Series(co["CLASS"])).T
                else:
                    print(co["@name"] + "はlist型でもdict型でもありません。")
                    continue
                CLASS.columns = list(map(lambda x: co["@name"] + "_" + x.lstrip("@"), CLASS.columns.values.tolist()))
                dfs.append(CLASS)
        else:
            print("CLASS_OBJはlist型ではありません。")

        return dfs

class StatsDataReader(_eStatReader):

    def __init__(
        self,
        api_key,
        header="name",
        explanationGetFlg=None,
        retry_count=3,
        pause=0.1,
        timeout=30,
        session=None,
        statsDataId="", 
        lvTab=None, 
        cdTab=None, 
        cdTabFrom=None, 
        cdTabTo=None, 
        lvTime=None, 
        cdTime=None, 
        cdTimeFrom=None, 
        cdTimeTo=None, 
        lvArea=None, 
        cdArea=None, 
        cdAreaFrom=None, 
        cdAreaTo=None, 
        lvCat01=None, 
        cdCat01=None, 
        cdCat01From=None, 
        cdCat01To=None, 
        lvCat02=None, 
        cdCat02=None, 
        cdCat02From=None, 
        cdCat02To=None, 
        lvCat03=None, 
        cdCat03=None, 
        cdCat03From=None, 
        cdCat03To=None, 
        startPosition=None, 
        limit=100000, 
        metaGetFlg=None, 
        cntGetFlg=None,
        annotationGetFlg=None,
        replaceSpChar=2,
        na_values=np.nan,
    ):

        super().__init__(
            api_key=api_key,
            explanationGetFlg=explanationGetFlg,
            retry_count=retry_count,
            pause=pause,
            timeout=timeout,
            session=session,
        )

        if api_key is None:
            api_key = os.getenv("ESTAT_API_KEY")
        if not api_key or not isinstance(api_key, str):
            raise ValueError(
                "The eStat API key must be provided either "
                "through the api_key variable or through the "
                "environmental variable ESTAT_API_KEY."
            )

        self.api_key = api_key
        self.header = header
        self.statsDataId = statsDataId
        self.lvTab = lvTab
        self.cdTab = cdTab
        self.cdTabFrom = cdTabFrom
        self.cdTabTo = cdTabTo
        self.lvTime = lvTime
        self.cdTime = cdTime
        self.cdTimeFrom = cdTimeFrom
        self.cdTimeTo = cdTimeTo
        self.lvArea = lvArea
        self.cdArea = cdArea
        self.cdAreaFrom = cdAreaFrom
        self.cdAreaTo = cdAreaTo
        self.lvCat01 = lvCat01
        self.cdCat01 = cdCat01
        self.cdCat01From = cdCat01From
        self.cdCat01To = cdCat01To
        self.lvCat02 = lvCat02
        self.cdCat02 = cdCat02
        self.cdCat02From = cdCat02From
        self.cdCat02To = cdCat02To
        self.lvCat03 = lvCat03
        self.cdCat03 = cdCat03
        self.cdCat03From = cdCat03From
        self.cdCat03To = cdCat03To
        self.startPosition = startPosition
        self.limit = limit
        self.metaGetFlg = metaGetFlg
        self.cntGetFlg = cntGetFlg
        self.explanationGetFlg = explanationGetFlg
        self.annotationGetFlg = annotationGetFlg
        self.replaceSpChar = replaceSpChar
        self.na_values = na_values

    @property
    def url(self):
        """API URL"""
        StatsList_URL = _BASE_URL + "/getStatsData?"
        return StatsList_URL

    @property
    def params(self):
        """Parameters to use in API calls"""
        pdict = {
            "appId": self.api_key,
        }
        
        if isinstance(self.statsDataId, (str, int)):
            pdict.update({"statsDataId": self.statsDataId})
        # 表章事項
        if isinstance(self.lvTab, (str, int)):
            pdict.update({"lvTab": self.lvTab})
        if isinstance(self.cdTab, (str, int)):
            pdict.update({"cdTab": self.cdTab})
        if isinstance(self.cdTabFrom, (str, int)):
            pdict.update({"cdTabFrom": self.cdTabFrom})
        if isinstance(self.cdTabTo, (str, int)):
            pdict.update({"cdTabTo": self.cdTabTo})
        # 時間軸事項
        if isinstance(self.lvTime, (str, int)):
            pdict.update({"lvTime": self.lvAlvTimerea})
        if isinstance(self.cdTime, (str, int)):
            pdict.update({"cdTime": self.cdTime})
        if isinstance(self.cdTimeFrom, (str, int)):
            pdict.update({"cdTimeFrom": self.cdTimeFrom})
        if isinstance(self.cdTimeTo, (str, int)):
            pdict.update({"cdTimeTo": self.cdTimeTo})
        # 地域事項
        if isinstance(self.lvArea, (str, int)):
            pdict.update({"lvArea": self.lvArea})
        if isinstance(self.cdArea, (str, int)):
            pdict.update({"cdArea": self.cdArea})
        if isinstance(self.cdAreaFrom, (str, int)):
            pdict.update({"cdAreaFrom": self.cdAreaFrom})
        if isinstance(self.cdAreaTo, (str, int)):
            pdict.update({"cdAreaTo": self.cdAreaTo})
        # 分類事項
        if isinstance(self.lvCat01, (str, int)):
            pdict.update({"lvCat01": self.lvCat01})
        if isinstance(self.cdCat01, (str, int)):
            pdict.update({"cdCat01": self.cdCat01})
        if isinstance(self.cdCat01From, (str, int)):
            pdict.update({"cdCat01From": self.cdCat01From})
        if isinstance(self.cdCat01To, (str, int)):
            pdict.update({"cdCat01To": self.cdCat01To})
        if isinstance(self.lvCat02, (str, int)):
            pdict.update({"lvCat02": self.lvCat02})
        if isinstance(self.cdCat02, (str, int)):
            pdict.update({"cdCat02": self.cdCat02})
        if isinstance(self.cdCat02From, (str, int)):
            pdict.update({"cdCat02From": self.cdCat02From})
        if isinstance(self.cdCat02To, (str, int)):
            pdict.update({"cdCat02To": self.cdCat02To})
        if isinstance(self.lvCat03, (str, int)):
            pdict.update({"lvCat03": self.lvCat03})
        if isinstance(self.cdCat03, (str, int)):
            pdict.update({"cdCat03": self.cdCat03})
        if isinstance(self.cdCat03From, (str, int)):
            pdict.update({"cdCat03From": self.cdCat03From})
        if isinstance(self.cdCat03To, (str, int)):
            pdict.update({"cdCat03To": self.cdCat03To})
        # データ取得開始位置
        if isinstance(self.startPosition, (str, int)):
            pdict.update({"startPosition": self.startPosition})
        if isinstance(self.limit, (str, int)):
            pdict.update({"limit": self.limit})
        # メタ情報有無
        if self.metaGetFlg in ["Y", "N"]:
            pdict.update({"metaGetFlg": self.metaGetFlg})
        # 件数取得フラグ
        if self.cntGetFlg in ["Y", "N"]:
            pdict.update({"cntGetFlg": self.cntGetFlg})
        if self.explanationGetFlg in ["Y", "N"]:
            pdict.update({"explanationGetFlg": self.explanationGetFlg})
        if self.annotationGetFlg in ["Y", "N"]:
            pdict.update({"annotationGetFlg": self.annotationGetFlg})
        if self.replaceSpChar in range(4):
            pdict.update({"replaceSpChar": self.replaceSpChar})
        
        return pdict

    def read(self, normal=True):
        """Read data from connector"""
        try:
            data = self._read(self.url, self.params)
            if normal:
                return data
            else:
                data["unit"] = data["unit"].fillna("")
                dims = self.attributes
                dims.remove("$")
                dfs = []
                for u in data["unit"].unique():
                    df = data[data["unit"]==u]
                    df = df.drop(dims + [c for c in df.columns if (("unit" in c) | ("level" in c))], axis=1)
                    df = df.set_index([c for c in df.columns if "name" in c]).unstack(self.cols_name)
                    df.columns = [l2 for l1, l2 in df.columns]
                    df = df.reset_index()
                    df.columns = list(map(lambda x: x.rstrip("_name"), df.columns.values.tolist()))
                    dfs.append(df)
                return dfs
        finally:
            self.close()

    def _read(self, url, params):
        if self.limit is None:
            out = self._get_response(url, params=dict(**params, **{"limit": 1})).json()
            OVERALL_TOTAL_NUMBER = out["GET_STATS_DATA"]["STATISTICAL_DATA"]["TABLE_INF"]["OVERALL_TOTAL_NUMBER"]
            if OVERALL_TOTAL_NUMBER > 100000:
                ptrans = {"tab": "cdTab",
                    "time": "cdTime",
                    "area": "cdArea",
                    "cat01": "cdCat01", 
                    "cat02": "cdCat02",
                    "cat03": "cdCat03",
                    "cat04": "cdCat04"}

                cls = pd.DataFrame([[n, co["@id"], len(co["CLASS"])] for n, co in enumerate(out["GET_STATS_DATA"]["STATISTICAL_DATA"]["CLASS_INF"]["CLASS_OBJ"]) if isinstance(co["CLASS"], list)]).sort_values(2, ascending=False)

                param_names = []
                codes = []
                tn = OVERALL_TOTAL_NUMBER
                for i in range(len(cls)):
                    param_names += [ptrans[cls.iloc[i, 1]]]
                    codes.append(list(pd.DataFrame(out["GET_STATS_DATA"]["STATISTICAL_DATA"]["CLASS_INF"]["CLASS_OBJ"][cls.iloc[i, 0]]["CLASS"])["@code"]))
                    tn //=  cls.iloc[i, 2]
                    if tn < 100000:
                        break

                codes_product = []
                pools = [tuple(pool) for pool in codes]
                result = [[]]
                for pool in pools:
                    result = [x+[y] for x in result for y in pool]
                for prod in result:
                    codes_product += [tuple(prod)]

                dfs = []
                for q in codes_product:
                    for n, p in enumerate(param_names):
                        params.update({p: q[n]})
                    try:
                        dfs.append(self._read_one_data(url, params))
                    finally:
                        self.close()
                return pd.concat(dfs, axis=0)

            else:
                return self._read_one_data(url, params)
        else:
            return self._read_one_data(url, params)

    def _read_one_data(self, url, params):
        """read one data from specified URL"""
        out = self._get_response(url, params=params).json()

        self.STATUS = out["GET_STATS_DATA"]["RESULT"]["STATUS"]
        self.ERROR_MSG = out["GET_STATS_DATA"]["RESULT"]["ERROR_MSG"]
        self.DATE = out["GET_STATS_DATA"]["RESULT"]["DATE"]
        self.LANG = out["GET_STATS_DATA"]["PARAMETER"]["LANG"]
        self.DATA_FORMAT = out["GET_STATS_DATA"]["PARAMETER"]["DATA_FORMAT"]
        self.START_POSITION = out["GET_STATS_DATA"]["PARAMETER"]["START_POSITION"]
        self.METAGET_FLG = out["GET_STATS_DATA"]["PARAMETER"]["METAGET_FLG"]
        self.TOTAL_NUMBER = out["GET_STATS_DATA"]["STATISTICAL_DATA"]["RESULT_INF"]["TOTAL_NUMBER"]
        self.FROM_NUMBER = out["GET_STATS_DATA"]["STATISTICAL_DATA"]["RESULT_INF"]["FROM_NUMBER"]
        self.TO_NUMBER = out["GET_STATS_DATA"]["STATISTICAL_DATA"]["RESULT_INF"]["TO_NUMBER"]
        self.TABLE_INF = out["GET_STATS_DATA"]["STATISTICAL_DATA"]["TABLE_INF"]
        self.STATISTICS_NAME = self.TABLE_INF["STATISTICS_NAME"]
        self.CYCLE = self.TABLE_INF["CYCLE"]
        self.OVERALL_TOTAL_NUMBER = self.TABLE_INF["OVERALL_TOTAL_NUMBER"]
        self.NOTE = out["GET_STATS_DATA"]["STATISTICAL_DATA"]["DATA_INF"]["NOTE"]

        VALUE = pd.DataFrame(out["GET_STATS_DATA"]["STATISTICAL_DATA"]["DATA_INF"]["VALUE"])
        self.attributes = list(map(lambda x: x.lstrip("@"), VALUE.columns.values.tolist()))
        VALUE.columns = self.attributes

        CLASS_OBJ = out["GET_STATS_DATA"]["STATISTICAL_DATA"]["CLASS_INF"]["CLASS_OBJ"]
        if isinstance(CLASS_OBJ, list):
            for co in CLASS_OBJ:
                CLASS = co["CLASS"]
                if isinstance(CLASS, list):
                    CLASS = pd.DataFrame(co["CLASS"])
                elif isinstance(CLASS, dict):
                    CLASS = pd.DataFrame(pd.Series(co["CLASS"])).T
                else:
                    continue
                CLASS = CLASS.set_index("@code")
                if self.header == "code":
                    CLASS.columns = list(map(lambda x: co["@id"] + "_" + x.lstrip("@"), CLASS.columns.values.tolist()))
                    self.cols_name = "tab_name"
                else:
                    CLASS.columns = list(map(lambda x: co["@name"] + "_" + x.lstrip("@"), CLASS.columns.values.tolist()))
                    self.cols_name = "表章項目_name"
                VALUE = VALUE.merge(CLASS, left_on=co["@id"], right_index=True, how="left")
        else:
            print("CLASS_OBJはlist型ではありません。")
            
        if isinstance(self.NOTE, list):
            note_char = [n["@char"] for n in self.NOTE]
            VALUE["$"] = VALUE["$"].replace(note_char, self.na_values)
        elif isinstance(self.NOTE, dict):
            note_char = self.NOTE["@char"]
            VALUE["$"] = VALUE["$"].replace(note_char, self.na_values)
        if np.isnan(self.na_values):
            VALUE["$"] =  VALUE["$"].astype(float)
            
        VALUE.rename(columns={"$": "value"}, inplace=True)
            
        if isinstance(self.TABLE_INF["TITLE"], dict):
            self.TITLE = self.TABLE_INF["TITLE"]["$"]
        else:
            self.TITLE = self.TABLE_INF["TITLE"]
        self.GOV_ORG = self.TABLE_INF["GOV_ORG"]["$"]
        if self.CYCLE != "-":
            self.StatsDataName = (self.STATISTICS_NAME + "_" + self.TITLE + "_" + self.CYCLE + "_" + self.GOV_ORG).replace(" ", "_")
        else:
            self.StatsDataName = (self.STATISTICS_NAME + "_" + self.TITLE + "_" + self.GOV_ORG).replace(" ", "_")
            
        return VALUE


class DataCatalogReader(_eStatReader):

    def __init__(
        self,
        api_key,
        explanationGetFlg=None,
        retry_count=3,
        pause=0.1,
        timeout=30,
        session=None,
        surveyYears=None, 
        openYears=None, 
        statsField=None, 
        statsCode=None, 
        searchWord=None, 
        collectArea=None, 
        dataType=None, 
        startPosition=None, 
        catalogId=None, 
        resourceId=None, 
        limit=100000, 
        updatedDate=None, 
    ):

        super().__init__(
            api_key=api_key,
            explanationGetFlg=explanationGetFlg,
            retry_count=retry_count,
            pause=pause,
            timeout=timeout,
            session=session,
        )

        if api_key is None:
            api_key = os.getenv("ESTAT_API_KEY")
        if not api_key or not isinstance(api_key, str):
            raise ValueError(
                "The eStat API key must be provided either "
                "through the api_key variable or through the "
                "environmental variable ESTAT_API_KEY."
            )

        self.api_key = api_key
        self.surveyYears = surveyYears
        self.openYears = openYears
        self.statsField = statsField
        self.statsCode = statsCode
        self.searchWord = searchWord
        self.collectArea = collectArea
        self.explanationGetFlg = explanationGetFlg
        self.dataType = dataType
        self.startPosition = startPosition
        self.catalogId = catalogId
        self.resourceId = resourceId
        self.limit = limit
        self.updatedDate = updatedDate

    @property
    def url(self):
        """API URL"""
        DataCatalog_URL = _BASE_URL + "/getDataCatalog?"
        return DataCatalog_URL

    @property
    def params(self):
        """Parameters to use in API calls"""
        pdict = {
            "appId": self.api_key,
        }
        
        if isinstance(self.surveyYears, (str, int)):
            pdict.update({"surveyYears": self.surveyYears})
        if isinstance(self.surveyYears, (str, int)):
            pdict.update({"surveyYears": self.surveyYears})
        if isinstance(self.openYears, (str, int)):
            pdict.update({"openYears": self.openYears})
        if isinstance(self.statsField, (str, int)):
            pdict.update({"statsField": self.statsField})
        if isinstance(self.statsCode, (str, int)):
            pdict.update({"statsCode": self.statsCode})
        if isinstance(self.searchWord, str):
            pdict.update({"searchWord": self.searchWord})
        if self.collectArea in range(1, 4):
            pdict.update({"collectArea": self.collectArea})
        if self.explanationGetFlg in ["Y", "N"]:
            pdict.update({"explanationGetFlg": self.explanationGetFlg})
        if self.dataType in ["XLS", "CSV", "PDF", "XML", "XLS_REP", "DB"]:
            pdict.update({"dataType": self.dataType})
        if isinstance(self.startPosition, (str, int)):
            pdict.update({"startPosition": self.startPosition})
        if isinstance(self.catalogId, (str, int)):
            pdict.update({"catalogId": self.catalogId})
        if isinstance(self.resourceId, (str, int)):
            pdict.update({"resourceId": self.resourceId})
        if isinstance(self.updatedDate, (str, int)):
            pdict.update({"updatedDate": self.updatedDate})
        
        return pdict

    def read(self):
        """Read data from connector"""
        try:
            return self._read_one_data(self.url, self.params)
        finally:
            self.close()

    def _read_one_data(self, url, params):
        """read one data from specified URL"""
        out = self._get_response(url, params=params).json()
        
        self.STATUS = out["GET_DATA_CATALOG"]["RESULT"]["STATUS"]
        self.ERROR_MSG = out["GET_DATA_CATALOG"]["RESULT"]["ERROR_MSG"]
        self.DATE = out["GET_DATA_CATALOG"]["RESULT"]["DATE"]
        self.LANG = out["GET_DATA_CATALOG"]["PARAMETER"]["LANG"]
        self.DATA_FORMAT = out["GET_DATA_CATALOG"]["PARAMETER"]["DATA_FORMAT"]
        self.NUMBER = out["GET_DATA_CATALOG"]["DATA_CATALOG_LIST_INF"]["NUMBER"]
        self.FROM_NUMBER = out["GET_DATA_CATALOG"]["DATA_CATALOG_LIST_INF"]["RESULT_INF"]["FROM_NUMBER"]
        self.TO_NUMBER = out["GET_DATA_CATALOG"]["DATA_CATALOG_LIST_INF"]["RESULT_INF"]["TO_NUMBER"]
        if "NEXT_KEY" in out["GET_DATA_CATALOG"]["DATA_CATALOG_LIST_INF"]["RESULT_INF"].keys():
            self.NEXT_KEY = out["GET_DATA_CATALOG"]["DATA_CATALOG_LIST_INF"]["RESULT_INF"]["NEXT_KEY"]

        DATA_CATALOG_INF = pd.json_normalize(out, record_path=["GET_DATA_CATALOG", "DATA_CATALOG_LIST_INF", "DATA_CATALOG_INF"], sep="_")
        DATA_CATALOG_INF.columns = list(map(lambda x: x.replace("@", "").rstrip("_$"), DATA_CATALOG_INF.columns.values.tolist()))

        return DATA_CATALOG_INF


