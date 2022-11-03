# -*- coding: utf-8 -*-

import os
import time
import urllib
import warnings

import numpy as np
import pandas as pd
import requests

from jpy_datareader.base import _BaseReader

_version = "3.0"
_BASE_URL = f"https://api.e-stat.go.jp/rest/{_version}/app/json"

attrdict = {
    "code": "コード",
    "name": "名",
    "level": "階層レベル",
    "unit": "単位",
    "parentCode": "親コード",
    "addInf": "追加情報",
    "tab": "表章項目",
    "cat": "分類",
    "area": "地域",
    "time": "時間軸",
    "annotation": "注釈記号",
}


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
                "environment variable ESTAT_API_KEY."
            )

        self.api_key = api_key
        self.explanationGetFlg = explanationGetFlg

    @property
    def url(self, path="getStatsList"):
        """API URL"""
        if path not in [
            "getStatsList",
            "getDataCatalog",
            "getMetaInfo",
            "getStatsData",
        ]:
            path = "getStatsList"
            print(
                "pathはgetStatsList, getDataCatalog, getMetaInfo, getStatsDataで指定します。pathをgetStatsListに置換しました。"
            )
        eStat_URL = _BASE_URL + f"/{path}?"
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
        url = "{url}{params}".format(url=self.url, params=paramstring)
        return url

    def rename_japanese(self, df):
        cols = []
        for c in df.columns:
            for k in attrdict.keys():
                c = c.replace(k, attrdict[k])
            cols += [c]
        df.columns = cols
        return df


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
        print(url)
        out = self._get_response(url, params=params).json()

        if "RESULT" in out["GET_STATS_LIST"].keys():
            if "STATUS" in out["GET_STATS_LIST"]["RESULT"].keys():
                self.STATUS = out["GET_STATS_LIST"]["RESULT"]["STATUS"]
            if "ERROR_MSG" in out["GET_STATS_LIST"]["RESULT"].keys():
                self.ERROR_MSG = out["GET_STATS_LIST"]["RESULT"]["ERROR_MSG"]
            if "DATE" in out["GET_STATS_LIST"]["RESULT"].keys():
                self.DATE = out["GET_STATS_LIST"]["RESULT"]["DATE"]
        if "PARAMETER" in out["GET_STATS_LIST"].keys():
            if "LANG" in out["GET_STATS_LIST"]["PARAMETER"].keys():
                self.LANG = out["GET_STATS_LIST"]["PARAMETER"]["LANG"]
            if "DATA_FORMAT" in out["GET_STATS_LIST"]["PARAMETER"].keys():
                self.DATA_FORMAT = out["GET_STATS_LIST"]["PARAMETER"]["DATA_FORMAT"]
        if "LIMIT" in out["GET_STATS_LIST"]["PARAMETER"].keys():
            self.LIMIT = out["GET_STATS_LIST"]["PARAMETER"]["LIMIT"]
        if "DATALIST_INF" in out["GET_STATS_LIST"].keys():
            if "NUMBER" in out["GET_STATS_LIST"]["DATALIST_INF"].keys():
                self.NUMBER = out["GET_STATS_LIST"]["DATALIST_INF"]["NUMBER"]
            if "RESULT_INF" in out["GET_STATS_LIST"]["DATALIST_INF"].keys():
                if (
                    "FROM_NUMBER"
                    in out["GET_STATS_LIST"]["DATALIST_INF"]["RESULT_INF"].keys()
                ):
                    self.FROM_NUMBER = out["GET_STATS_LIST"]["DATALIST_INF"][
                        "RESULT_INF"
                    ]["FROM_NUMBER"]
                if (
                    "TO_NUMBER"
                    in out["GET_STATS_LIST"]["DATALIST_INF"]["RESULT_INF"].keys()
                ):
                    self.TO_NUMBER = out["GET_STATS_LIST"]["DATALIST_INF"][
                        "RESULT_INF"
                    ]["TO_NUMBER"]
                if (
                    "NEXT_KEY"
                    in out["GET_STATS_LIST"]["DATALIST_INF"]["RESULT_INF"].keys()
                ):
                    self.NEXT_KEY = out["GET_STATS_LIST"]["DATALIST_INF"]["RESULT_INF"][
                        "NEXT_KEY"
                    ]

        TABLE_INF = pd.json_normalize(
            out, record_path=["GET_STATS_LIST", "DATALIST_INF", "TABLE_INF"], sep="_"
        )
        TABLE_INF.columns = list(
            map(
                lambda x: x.replace("@", "").rstrip("_$"),
                TABLE_INF.columns.values.tolist(),
            )
        )

        return TABLE_INF


class MetaInfoReader(_eStatReader):
    def __init__(
        self,
        api_key,
        statsDataId,
        name_or_id="name",
        lvhierarchy=False,
        lvfillna=False,
        explanationGetFlg=None,
        retry_count=3,
        pause=0.1,
        timeout=30,
        session=None,
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
        self.name_or_id = name_or_id
        self.lvhierarchy = lvhierarchy
        self.lvfillna = lvfillna
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
        if "PARAMETER" in out["GET_META_INFO"].keys():
            if "LANG" in out["GET_META_INFO"]["PARAMETER"].keys():
                self.LANG = out["GET_META_INFO"]["PARAMETER"]["LANG"]
            if "DATA_FORMAT" in out["GET_META_INFO"]["PARAMETER"].keys():
                self.DATA_FORMAT = out["GET_META_INFO"]["PARAMETER"]["DATA_FORMAT"]
        if "METADATA_INF" in out["GET_META_INFO"].keys():
            if "TABLE_INF" in out["GET_META_INFO"]["METADATA_INF"].keys():
                self.TABLE_INF = out["GET_META_INFO"]["METADATA_INF"]["TABLE_INF"]
                if "STAT_NAME" in self.TABLE_INF.keys():
                    self.STAT_NAME = self.TABLE_INF["STAT_NAME"]
                if "GOV_ORG" in self.TABLE_INF.keys():
                    self.GOV_ORG = self.TABLE_INF["GOV_ORG"]
                if "STATISTICS_NAME" in self.TABLE_INF.keys():
                    self.STATISTICS_NAME = self.TABLE_INF["STATISTICS_NAME"]
                if "TITLE" in self.TABLE_INF.keys():
                    self.TITLE = self.TABLE_INF["TITLE"]
                if "CYCLE" in self.TABLE_INF.keys():
                    self.CYCLE = self.TABLE_INF["CYCLE"]
                if "SURVEY_DATE" in self.TABLE_INF.keys():
                    self.SURVEY_DATE = self.TABLE_INF["SURVEY_DATE"]
                if "OPEN_DATE" in self.TABLE_INF.keys():
                    self.OPEN_DATE = self.TABLE_INF["OPEN_DATE"]
                if "SMALL_AREA" in self.TABLE_INF.keys():
                    self.SMALL_AREA = self.TABLE_INF["SMALL_AREA"]
                if "COLLECT_AREA" in self.TABLE_INF.keys():
                    self.COLLECT_AREA = self.TABLE_INF["COLLECT_AREA"]
                if "MAIN_CATEGORY" in self.TABLE_INF.keys():
                    self.MAIN_CATEGORY = self.TABLE_INF["MAIN_CATEGORY"]
                if "SUB_CATEGORY" in self.TABLE_INF.keys():
                    self.SUB_CATEGORY = self.TABLE_INF["SUB_CATEGORY"]
                if "OVERALL_TOTAL_NUMBER" in self.TABLE_INF.keys():
                    self.OVERALL_TOTAL_NUMBER = self.TABLE_INF["OVERALL_TOTAL_NUMBER"]
                if "UPDATED_DATE" in self.TABLE_INF.keys():
                    self.UPDATED_DATE = self.TABLE_INF["UPDATED_DATE"]
                if "STATISTICS_NAME_SPEC" in self.TABLE_INF.keys():
                    self.STATISTICS_NAME_SPEC = self.TABLE_INF["STATISTICS_NAME_SPEC"]
                if "TABULATION_SUB_CATEGORY1" in self.TABLE_INF.keys():
                    self.TABULATION_SUB_CATEGORY1 = self.TABLE_INF[
                        "TABULATION_SUB_CATEGORY1"
                    ]
                if "DESCRIPTION" in self.TABLE_INF.keys():
                    self.DESCRIPTION = self.TABLE_INF["DESCRIPTION"]
                if "TITLE_SPEC" in self.TABLE_INF.keys():
                    self.TITLE_SPEC = self.TABLE_INF["TITLE_SPEC"]

        self.CLASS_OBJ = out["GET_META_INFO"]["METADATA_INF"]["CLASS_INF"]["CLASS_OBJ"]

        dfs = {}
        if isinstance(self.CLASS_OBJ, list):
            for co in self.CLASS_OBJ:
                CLASS = co["CLASS"]
                if isinstance(CLASS, list):
                    CLASS = pd.DataFrame(co["CLASS"])
                elif isinstance(CLASS, dict):
                    CLASS = pd.DataFrame(pd.Series(co["CLASS"])).T
                else:
                    print(co["@name"] + "はlist型でもdict型でもありません。")
                CLASS.columns = list(
                    map(lambda x: x.lstrip("@"), CLASS.columns.values.tolist())
                )

                is_hierarchy = self.lvhierarchy & (len(CLASS["level"].unique()) > 1)
                if is_hierarchy:
                    levels = self.hierarchy_level(CLASS, co["@id"])

                if self.name_or_id == "name":
                    CLASS.columns = list(
                        map(lambda x: co["@name"] + x, CLASS.columns.values.tolist())
                    )
                    CLASS = self.rename_japanese(CLASS)
                else:
                    CLASS.columns = list(
                        map(
                            lambda x: co["@id"] + "_" + x, CLASS.columns.values.tolist()
                        )
                    )

                if is_hierarchy:
                    dfs[co["@id"]] = [CLASS, levels]
                else:
                    dfs[co["@id"]] = CLASS

        else:
            print("CLASS_OBJはlist型ではありません。")

        return dfs

    def hierarchy_level(self, df, id):
        levels = df[df["level"] == "1"][["code"]]
        levels.columns = ["level1"]
        for lv in np.sort(df["level"].unique().astype(int))[1:]:
            child = df[df["level"] == str(lv)][["parentCode", "code"]]
            child.columns = ["level" + str(lv - 1), "level" + str(lv)]
            levels = levels.merge(child, on="level" + str(lv - 1), how="left")
        if self.lvfillna:
            levels = levels.fillna(method="ffill", axis=1)
        levels.columns = list(
            map(lambda x: id + "_" + x, levels.columns.values.tolist())
        )
        return levels


class StatsDataReader(_eStatReader):
    """
    Get data for the given name from eStat.

    .. versionadded:: 3.0

    Parameters
    ----------
    api_key : str, optional
        eStat API key.
        取得したアプリケーションID(appId)を指定.
    name_or_id : "name" or "id"
    """

    def __init__(
        self,
        api_key,
        statsDataId,
        name_or_id="name",
        explanationGetFlg=None,
        retry_count=3,
        pause=0.1,
        timeout=30,
        session=None,
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
        self.name_or_id = name_or_id
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

    def read(self, normal=True, split_units=False):
        """Read data from connector"""
        try:
            data = self._read(self.url, self.params)
            if normal:
                return data
            else:

                def denormalization(input, header):
                    if header == "name":
                        df = input.drop(
                            [
                                c
                                for c in input.columns
                                if (
                                    ("コード" in c)
                                    | ("階層レベル" in c)
                                    | ("unit" in c)
                                    | ("単位" in c)
                                    | ("親コード" in c)
                                    | ("追加情報" in c)
                                )
                            ],
                            axis=1,
                        )
                    else:
                        df = input.drop(
                            [
                                c
                                for c in input.columns
                                if (
                                    ("code" in c)
                                    | ("level" in c)
                                    | ("unit" in c)
                                    | ("parentCode" in c)
                                    | ("addInf" in c)
                                )
                            ],
                            axis=1,
                        )

                    if self.tabcol in df.index.names:
                        if header == "name":
                            df = df.set_index([c for c in df.columns if "名" in c])
                        else:
                            df = df.set_index([c for c in df.columns if "name" in c])
                        df = df.unstack(self.tabcol)
                        df.columns = [l2 for l1, l2 in df.columns]
                        df = df.reset_index()

                    df.columns = list(
                        map(
                            lambda x: x.replace("_name", "").rstrip("名"),
                            df.columns.values.tolist(),
                        )
                    )
                    return df

                if split_units:
                    if "単位コード" in data.columns:
                        data["単位コード"] = data["単位コード"].fillna("単位なし")
                        data.rename(columns={"単位コード": "unit"}, inplace=True)
                    elif "unit_code" in data.columns:
                        data["unit_code"] = data["unit_code"].fillna("単位なし")
                        data.rename(columns={"unit_code": "unit"}, inplace=True)

                    datasets = {}
                    for u in data["unit"].unique():
                        df = data[data["unit"] == u]
                        datasets[u] = denormalization(df, self.name_or_id)
                    return datasets
                else:
                    return denormalization(data, self.name_or_id)

        finally:
            self.close()

    def _read(self, url, params):
        if self.limit is None:
            out = self._get_response(url, params=dict(**params, **{"limit": 1})).json()
            OVERALL_TOTAL_NUMBER = out["GET_STATS_DATA"]["STATISTICAL_DATA"][
                "TABLE_INF"
            ]["OVERALL_TOTAL_NUMBER"]

            if OVERALL_TOTAL_NUMBER > 100000:
                ptrans = {
                    "tab": "cdTab",
                    "time": "cdTime",
                    "area": "cdArea",
                    "cat01": "cdCat01",
                    "cat02": "cdCat02",
                    "cat03": "cdCat03",
                    "cat04": "cdCat04",
                }

                cls = pd.DataFrame(
                    [
                        [n, co["@id"], len(co["CLASS"])]
                        for n, co in enumerate(
                            out["GET_STATS_DATA"]["STATISTICAL_DATA"]["CLASS_INF"][
                                "CLASS_OBJ"
                            ]
                        )
                        if isinstance(co["CLASS"], list)
                    ]
                ).sort_values(2, ascending=False)

                param_names = []
                codes = []
                tn = OVERALL_TOTAL_NUMBER
                for i in range(len(cls)):
                    param_names += [ptrans[cls.iloc[i, 1]]]
                    codes.append(
                        list(
                            pd.DataFrame(
                                out["GET_STATS_DATA"]["STATISTICAL_DATA"]["CLASS_INF"][
                                    "CLASS_OBJ"
                                ][cls.iloc[i, 0]]["CLASS"]
                            )["@code"]
                        )
                    )
                    tn //= cls.iloc[i, 2]
                    if tn < 100000:
                        break

                codes_product = []
                pools = [tuple(pool) for pool in codes]
                result = [[]]
                for pool in pools:
                    result = [x + [y] for x in result for y in pool]
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
                return pd.concat(dfs, axis=0).reset_index(drop=True)

            else:
                return self._read_one_data(url, params)
        else:
            return self._read_one_data(url, params)

    def _read_one_data(self, url, params):
        """read one data from specified URL"""
        out = self._get_response(url, params=params).json()

        if "RESULT" in out["GET_STATS_DATA"].keys():
            if "STATUS" in out["GET_STATS_DATA"]["RESULT"].keys():
                self.STATUS = out["GET_STATS_DATA"]["RESULT"]["STATUS"]
            if "ERROR_MSG" in out["GET_STATS_DATA"]["RESULT"].keys():
                self.ERROR_MSG = out["GET_STATS_DATA"]["RESULT"]["ERROR_MSG"]
            if "DATE" in out["GET_STATS_DATA"]["RESULT"].keys():
                self.DATE = out["GET_STATS_DATA"]["RESULT"]["DATE"]

        VALUE = pd.DataFrame(
            out["GET_STATS_DATA"]["STATISTICAL_DATA"]["DATA_INF"]["VALUE"]
        )
        self.attrlist = list(
            map(lambda x: x.lstrip("@"), VALUE.columns.values.tolist())
        )
        VALUE.columns = self.attrlist

        if "PARAMETER" in out["GET_STATS_DATA"].keys():
            if "LANG" in out["GET_STATS_DATA"]["PARAMETER"].keys():
                self.LANG = out["GET_STATS_DATA"]["PARAMETER"]["LANG"]
            if "DATA_FORMAT" in out["GET_STATS_DATA"]["PARAMETER"].keys():
                self.DATA_FORMAT = out["GET_STATS_DATA"]["PARAMETER"]["DATA_FORMAT"]
            if "START_POSITION" in out["GET_STATS_DATA"]["PARAMETER"].keys():
                self.START_POSITION = out["GET_STATS_DATA"]["PARAMETER"][
                    "START_POSITION"
                ]
            if "METAGET_FLG" in out["GET_STATS_DATA"]["PARAMETER"].keys():
                self.METAGET_FLG = out["GET_STATS_DATA"]["PARAMETER"]["METAGET_FLG"]
        if "STATISTICAL_DATA" in out["GET_STATS_DATA"].keys():
            if "RESULT_INF" in out["GET_STATS_DATA"]["STATISTICAL_DATA"].keys():
                if (
                    "TOTAL_NUMBER"
                    in out["GET_STATS_DATA"]["STATISTICAL_DATA"]["RESULT_INF"].keys()
                ):
                    self.TOTAL_NUMBER = out["GET_STATS_DATA"]["STATISTICAL_DATA"][
                        "RESULT_INF"
                    ]["TOTAL_NUMBER"]
                if (
                    "FROM_NUMBER"
                    in out["GET_STATS_DATA"]["STATISTICAL_DATA"]["RESULT_INF"].keys()
                ):
                    self.FROM_NUMBER = out["GET_STATS_DATA"]["STATISTICAL_DATA"][
                        "RESULT_INF"
                    ]["FROM_NUMBER"]
                if (
                    "TO_NUMBER"
                    in out["GET_STATS_DATA"]["STATISTICAL_DATA"]["RESULT_INF"].keys()
                ):
                    self.TO_NUMBER = out["GET_STATS_DATA"]["STATISTICAL_DATA"][
                        "RESULT_INF"
                    ]["TO_NUMBER"]
            if "TABLE_INF" in out["GET_STATS_DATA"]["STATISTICAL_DATA"].keys():
                self.TABLE_INF = out["GET_STATS_DATA"]["STATISTICAL_DATA"]["TABLE_INF"]
                if "STATISTICS_NAME" in self.TABLE_INF.keys():
                    self.STATISTICS_NAME = self.TABLE_INF["STATISTICS_NAME"]
                if "CYCLE" in self.TABLE_INF.keys():
                    self.CYCLE = self.TABLE_INF["CYCLE"]
                if "OVERALL_TOTAL_NUMBER" in self.TABLE_INF.keys():
                    self.OVERALL_TOTAL_NUMBER = self.TABLE_INF["OVERALL_TOTAL_NUMBER"]
            if "DATA_INF" in out["GET_STATS_DATA"]["STATISTICAL_DATA"].keys():
                if (
                    "NOTE"
                    in out["GET_STATS_DATA"]["STATISTICAL_DATA"]["DATA_INF"].keys()
                ):
                    self.NOTE = out["GET_STATS_DATA"]["STATISTICAL_DATA"]["DATA_INF"][
                        "NOTE"
                    ]
                    if isinstance(self.NOTE, list):
                        note_char = [n["@char"] for n in self.NOTE]
                        VALUE["$"] = VALUE["$"].replace(note_char, self.na_values)
                    elif isinstance(self.NOTE, dict):
                        note_char = self.NOTE["@char"]
                        VALUE["$"] = VALUE["$"].replace(note_char, self.na_values)
                    if np.isnan(self.na_values):
                        VALUE["$"] = VALUE["$"].astype(float)

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
                if self.name_or_id == "id":
                    CLASS.columns = list(
                        map(
                            lambda x: co["@id"] + "_" + x.lstrip("@"),
                            CLASS.columns.values.tolist(),
                        )
                    )
                    self.tabcol = "tab_name"
                    VALUE = VALUE.merge(
                        CLASS, left_on=co["@id"], right_index=True, how="left"
                    )
                else:
                    CLASS.columns = list(
                        map(
                            lambda x: co["@name"] + x.lstrip("@"),
                            CLASS.columns.values.tolist(),
                        )
                    )
                    self.tabcol = "表章項目名"
                    VALUE = VALUE.merge(
                        CLASS, left_on=co["@id"], right_index=True, how="left"
                    )
        else:
            print("CLASS_OBJはlist型ではありません。")

        VALUE.columns = VALUE.columns = [
            c + "_code" if c in self.attrlist else c for c in VALUE.columns
        ]
        VALUE.rename(columns={"$_code": "value"}, inplace=True)
        if self.name_or_id == "name":
            VALUE = self.rename_japanese(VALUE)
            VALUE.rename(columns={"value": "値"}, inplace=True)

        if isinstance(self.TABLE_INF["TITLE"], dict):
            self.TITLE = self.TABLE_INF["TITLE"]["$"]
        else:
            self.TITLE = self.TABLE_INF["TITLE"]
        self.GOV_ORG = self.TABLE_INF["GOV_ORG"]["$"]
        if self.CYCLE != "-":
            self.StatsDataName = (
                self.STATISTICS_NAME
                + "_"
                + self.TITLE
                + "_"
                + self.CYCLE
                + "_"
                + self.GOV_ORG
            ).replace(" ", "_")
        else:
            self.StatsDataName = (
                self.STATISTICS_NAME + "_" + self.TITLE + "_" + self.GOV_ORG
            ).replace(" ", "_")

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
        self.FROM_NUMBER = out["GET_DATA_CATALOG"]["DATA_CATALOG_LIST_INF"][
            "RESULT_INF"
        ]["FROM_NUMBER"]
        self.TO_NUMBER = out["GET_DATA_CATALOG"]["DATA_CATALOG_LIST_INF"]["RESULT_INF"][
            "TO_NUMBER"
        ]
        if (
            "NEXT_KEY"
            in out["GET_DATA_CATALOG"]["DATA_CATALOG_LIST_INF"]["RESULT_INF"].keys()
        ):
            self.NEXT_KEY = out["GET_DATA_CATALOG"]["DATA_CATALOG_LIST_INF"][
                "RESULT_INF"
            ]["NEXT_KEY"]

        DATA_CATALOG_INF = pd.json_normalize(
            out,
            record_path=[
                "GET_DATA_CATALOG",
                "DATA_CATALOG_LIST_INF",
                "DATA_CATALOG_INF",
            ],
            sep="_",
        )
        DATA_CATALOG_INF.columns = list(
            map(
                lambda x: x.replace("@", "").rstrip("_$"),
                DATA_CATALOG_INF.columns.values.tolist(),
            )
        )

        return DATA_CATALOG_INF
