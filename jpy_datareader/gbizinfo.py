# -*- coding: utf-8 -*-

import os
import time

import warnings

import requests

import pandas as pd

from jpy_datareader.base import _BaseReader

_version = "v1"
_BASE_URL = f"https://info.gbiz.go.jp/hojin/{_version}/hojin"

class _gBizInfoReader(_BaseReader):
    """
    
    """
    def __init__(
        self,
        retry_count=3,
        pause=0.1,
        timeout=30,
        session=None,
        api_key=None,
    ):

        super().__init__(
            retry_count=retry_count,
            pause=pause,
            timeout=timeout,
            session=session,
        )

        if api_key is None:
            api_key = os.getenv("GBIZINFO_API_KEY")
        if not api_key or not isinstance(api_key, str):
            raise ValueError(
                "The gBizINFO API key must be provided either "
                "through the api_key variable or through the "
                "environmental variable GBIZINFO_API_KEY."
            )

        self.api_key = api_key

    @property
    def url(self):
        """API URL"""
        URL = _BASE_URL + "?"
        return URL

    @property
    def params(self):
        """Parameters to use in API calls"""
        hdict = {
            "Accept": "application/json",
            "X-hojinInfo-api-token": self.api_key,
        }
        return hdict

    def read(self):
        """Read data from connector"""
        try:
            return self._read_one_data(self.url, self.params)
        finally:
            self.close()

    def _read_one_data(self, url, params):
        """read one data from specified URL"""
        out = self._get_response(url, headers=params).json()
        hojin_infos = pd.json_normalize(out, record_path=["hojin-infos"], sep="_")
        return hojin_infos


class hojinReader(_gBizInfoReader):

    def __init__(
        self,
        api_key,
        corporate_number=None,
        name=None,
        exist_flg=None,
        corporate_type=None,
        prefecture=None,
        city=None,
        capital_stock_from=None,
        capital_stock_to=None,
        employee_number_from=None,
        employee_number_to=None,
        founded_year=None,
        sales_area=None,
        business_item=None,
        unified_qualification=None,
        unified_qualification_sub01=None,
        unified_qualification_sub02=None,
        unified_qualification_sub03=None,
        unified_qualification_sub04=None,
        net_sales_summary_of_business_results_from=None,
        net_sales_summary_of_business_results_to=None,
        net_income_loss_summary_of_business_results_from=None,
        net_income_loss_summary_of_business_results_to=None,
        total_assets_summary_of_business_results_from=None,
        total_assets_summary_of_business_results_to=None,
        name_major_shareholders=None,
        average_continuous_service_years=None,
        average_age=None,
        month_average_predetermined_overtime_hours=None,
        female_workers_proportion=None,
        year=None,
        ministry=None,
        source=None,
        page=1,
        limit=5000,
        retry_count=3,
        pause=0.1,
        timeout=30,
        session=None,
    ):

        super().__init__(
            api_key=api_key,
            retry_count=retry_count,
            pause=pause,
            timeout=timeout,
            session=session,
        )

        if api_key is None:
            api_key = os.getenv("GBIZINFO_API_KEY")
        if not api_key or not isinstance(api_key, str):
            raise ValueError(
                "The gBizINFO API key must be provided either "
                "through the api_key variable or through the "
                "environmental variable GBIZINFO_API_KEY."
            )

        self.api_key = api_key
        self.corporate_number = corporate_number
        self.name = name
        self.exist_flg = exist_flg
        self.corporate_type = corporate_type
        self.prefecture = prefecture
        self.city = city
        self.capital_stock_from = capital_stock_from
        self.capital_stock_to = capital_stock_to
        self.employee_number_from = employee_number_from
        self.employee_number_to = employee_number_to
        self.founded_year = founded_year
        self.sales_area = sales_area
        self.business_item = business_item
        self.unified_qualification = unified_qualification
        self.unified_qualification_sub01 = unified_qualification_sub01
        self.unified_qualification_sub02 = unified_qualification_sub02
        self.unified_qualification_sub03 = unified_qualification_sub03
        self.unified_qualification_sub04 = unified_qualification_sub04
        self.net_sales_summary_of_business_results_from = net_sales_summary_of_business_results_from
        self.net_sales_summary_of_business_results_to = net_sales_summary_of_business_results_to
        self.net_income_loss_summary_of_business_results_from = net_income_loss_summary_of_business_results_from
        self.net_income_loss_summary_of_business_results_to = net_income_loss_summary_of_business_results_to
        self.total_assets_summary_of_business_results_from = total_assets_summary_of_business_results_from
        self.total_assets_summary_of_business_results_to = total_assets_summary_of_business_results_to
        self.name_major_shareholders = name_major_shareholders
        self.average_continuous_service_years = average_continuous_service_years
        self.average_age = average_age
        self.month_average_predetermined_overtime_hours = month_average_predetermined_overtime_hours
        self.female_workers_proportion = female_workers_proportion
        self.year = year
        self.ministry = ministry
        self.source = source
        self.page = page
        self.limit = limit

    @property
    def url(self):
        """API URL"""
        hojin_URL = _BASE_URL + "?"
        return hojin_URL

    @property
    def params(self):
        """Parameters to use in API calls"""
        pdict = {}
        
        if isinstance(self.corporate_number, (str, int)):
            pdict.update({"corporate_number": self.corporate_number})
        if isinstance(self.name, str):
            pdict.update({"name": self.name})
        if isinstance(self.exist_flg, str):
            pdict.update({"exist_flg": self.exist_flg})
        if isinstance(self.corporate_type, str):
            pdict.update({"corporate_type": self.corporate_type})
        if isinstance(self.prefecture, str):
            pdict.update({"prefecture": self.prefecture})
        if isinstance(self.city, str):
            pdict.update({"city": self.city})
        if isinstance(self.capital_stock_from, (str, int)):
            pdict.update({"capital_stock_from": self.capital_stock_from})
        if isinstance(self.capital_stock_to, (str, int)):
            pdict.update({"capital_stock_to": self.capital_stock_to})
        if isinstance(self.employee_number_from, (str, int)):
            pdict.update({"employee_number_from": self.employee_number_from})
        if isinstance(self.employee_number_to, (str, int)):
            pdict.update({"employee_number_to": self.employee_number_to})
        if isinstance(self.founded_year, (str, int)):
            pdict.update({"founded_year": self.founded_year})
        if isinstance(self.sales_area, str):
            pdict.update({"sales_area": self.sales_area})
        if isinstance(self.business_item, str):
            pdict.update({"business_item": self.business_item})
        if isinstance(self.unified_qualification, str):
            pdict.update({"unified_qualification": self.unified_qualification})
        if isinstance(self.unified_qualification_sub01, str):
            pdict.update({"unified_qualification_sub01": self.unified_qualification_sub01})
        if isinstance(self.unified_qualification_sub02, str):
            pdict.update({"unified_qualification_sub02": self.unified_qualification_sub02})
        if isinstance(self.unified_qualification_sub03, str):
            pdict.update({"unified_qualification_sub03": self.unified_qualification_sub03})
        if isinstance(self.unified_qualification_sub04, str):
            pdict.update({"unified_qualification_sub04": self.unified_qualification_sub04})
        if isinstance(self.net_sales_summary_of_business_results_from, (str, int)):
            pdict.update({"net_sales_summary_of_business_results_from": self.net_sales_summary_of_business_results_from})
        if isinstance(self.net_sales_summary_of_business_results_to, (str, int)):
            pdict.update({"net_sales_summary_of_business_results_to": self.net_sales_summary_of_business_results_to})
        if isinstance(self.net_income_loss_summary_of_business_results_from, (str, int)):
            pdict.update({"net_income_loss_summary_of_business_results_from": self.net_income_loss_summary_of_business_results_from})
        if isinstance(self.net_income_loss_summary_of_business_results_to, (str, int)):
            pdict.update({"net_income_loss_summary_of_business_results_to": self.net_income_loss_summary_of_business_results_to})
        if isinstance(self.total_assets_summary_of_business_results_from, (str, int)):
            pdict.update({"total_assets_summary_of_business_results_from": self.total_assets_summary_of_business_results_from})
        if isinstance(self.total_assets_summary_of_business_results_to, (str, int)):
            pdict.update({"total_assets_summary_of_business_results_to": self.total_assets_summary_of_business_results_to})
        if isinstance(self.name_major_shareholders, str):
            pdict.update({"name_major_shareholders": self.name_major_shareholders})
        if isinstance(self.average_continuous_service_years, (str, int)):
            pdict.update({"average_continuous_service_years": self.average_continuous_service_years})
        if isinstance(self.average_age, (str, int)):
            pdict.update({"average_age": self.average_age})
        if isinstance(self.month_average_predetermined_overtime_hours, (str, int)):
            pdict.update({"month_average_predetermined_overtime_hours": self.month_average_predetermined_overtime_hours})
        if isinstance(self.female_workers_proportion, str):
            pdict.update({"female_workers_proportion": self.female_workers_proportion})
        if isinstance(self.year, (str, int)):
            pdict.update({"year": self.year})
        if isinstance(self.ministry, str):
            pdict.update({"ministry": self.ministry})
        if isinstance(self.source, (str, int)):
            pdict.update({"source": self.source})
        if isinstance(self.page, (str, int)):
            pdict.update({"page": self.page})
        if isinstance(self.limit, (str, int)):
            pdict.update({"limit": self.limit})

        return pdict


    def read(self):
        """Read data from connector"""
        try:
            return self._read_one_data(self.url, self.params)
        finally:
            self.close()

    def _read_one_data(self, url, params):
        """read one data from specified URL"""
        hdict = {
            "Accept": "application/json",
            "X-hojinInfo-api-token": self.api_key,
        }
        out = self._get_response(url, params=params, headers=hdict).json()
        hojin_infos = pd.json_normalize(out, record_path=["hojin-infos"], sep="_")
        return hojin_infos


class corporate_naumberReader(_gBizInfoReader):

    def __init__(
        self,
        api_key,
        corporate_number,
        retry_count=3,
        pause=0.1,
        timeout=30,
        session=None,
    ):

        super().__init__(
            api_key=api_key,
            retry_count=retry_count,
            pause=pause,
            timeout=timeout,
            session=session,
        )

        if api_key is None:
            api_key = os.getenv("GBIZINFO_API_KEY")
        if not api_key or not isinstance(api_key, str):
            raise ValueError(
                "The gBizINFO API key must be provided either "
                "through the api_key variable or through the "
                "environmental variable GBIZINFO_API_KEY."
            )

        self.api_key = api_key
        self.corporate_number = corporate_number

    @property
    def url(self):
        """API URL"""
        corporate_number_URL = _BASE_URL + f"{self.corporate_number}?"
        return corporate_number_URL


class certificationReader(_gBizInfoReader):

    def __init__(
        self,
        api_key,
        corporate_number,
        retry_count=3,
        pause=0.1,
        timeout=30,
        session=None,
    ):

        super().__init__(
            api_key=api_key,
            retry_count=retry_count,
            pause=pause,
            timeout=timeout,
            session=session,
        )

        if api_key is None:
            api_key = os.getenv("GBIZINFO_API_KEY")
        if not api_key or not isinstance(api_key, str):
            raise ValueError(
                "The gBizINFO API key must be provided either "
                "through the api_key variable or through the "
                "environmental variable GBIZINFO_API_KEY."
            )

        self.api_key = api_key
        self.corporate_number = corporate_number

    @property
    def url(self):
        certification_URL = _BASE_URL + f"/{self.corporate_number}/certification"
        return certification_URL


class commendationReader(_gBizInfoReader):

    def __init__(
        self,
        api_key,
        corporate_number,
        retry_count=3,
        pause=0.1,
        timeout=30,
        session=None,
    ):

        super().__init__(
            api_key=api_key,
            retry_count=retry_count,
            pause=pause,
            timeout=timeout,
            session=session,
        )

        if api_key is None:
            api_key = os.getenv("GBIZINFO_API_KEY")
        if not api_key or not isinstance(api_key, str):
            raise ValueError(
                "The gBizINFO API key must be provided either "
                "through the api_key variable or through the "
                "environmental variable GBIZINFO_API_KEY."
            )

        self.api_key = api_key
        self.corporate_number = corporate_number

    @property
    def url(self):
        """API URL"""
        commendation_URL = _BASE_URL + f"/{self.corporate_number}/commendation?"
        return commendation_URL


class financeReader(_gBizInfoReader):

    def __init__(
        self,
        api_key,
        corporate_number,
        retry_count=3,
        pause=0.1,
        timeout=30,
        session=None,
    ):

        super().__init__(
            api_key=api_key,
            retry_count=retry_count,
            pause=pause,
            timeout=timeout,
            session=session,
        )

        if api_key is None:
            api_key = os.getenv("GBIZINFO_API_KEY")
        if not api_key or not isinstance(api_key, str):
            raise ValueError(
                "The gBizINFO API key must be provided either "
                "through the api_key variable or through the "
                "environmental variable GBIZINFO_API_KEY."
            )

        self.api_key = api_key
        self.corporate_number = corporate_number

    @property
    def url(self):
        """API URL"""
        finance_URL = _BASE_URL + f"/{self.corporate_number}/finance?"
        return finance_URL
    

class patentReader(_gBizInfoReader):

    def __init__(
        self,
        api_key,
        corporate_number,
        retry_count=3,
        pause=0.1,
        timeout=30,
        session=None,
    ):

        super().__init__(
            api_key=api_key,
            retry_count=retry_count,
            pause=pause,
            timeout=timeout,
            session=session,
        )

        if api_key is None:
            api_key = os.getenv("GBIZINFO_API_KEY")
        if not api_key or not isinstance(api_key, str):
            raise ValueError(
                "The gBizINFO API key must be provided either "
                "through the api_key variable or through the "
                "environmental variable GBIZINFO_API_KEY."
            )

        self.api_key = api_key
        self.corporate_number = corporate_number

    @property
    def url(self):
        """API URL"""
        patent_URL = _BASE_URL + f"/{self.corporate_number}/patent?"
        return patent_URL

class procurementReader(_gBizInfoReader):

    def __init__(
        self,
        api_key,
        corporate_number,
        retry_count=3,
        pause=0.1,
        timeout=30,
        session=None,
    ):

        super().__init__(
            api_key=api_key,
            retry_count=retry_count,
            pause=pause,
            timeout=timeout,
            session=session,
        )

        if api_key is None:
            api_key = os.getenv("GBIZINFO_API_KEY")
        if not api_key or not isinstance(api_key, str):
            raise ValueError(
                "The gBizINFO API key must be provided either "
                "through the api_key variable or through the "
                "environmental variable GBIZINFO_API_KEY."
            )

        self.api_key = api_key
        self.corporate_number = corporate_number

    @property
    def url(self):
        """API URL"""
        procurement_URL = _BASE_URL + f"/{self.corporate_number}/procurement?"
        return procurement_URL

class subsidyReader(_gBizInfoReader):

    def __init__(
        self,
        api_key,
        corporate_number,
        retry_count=3,
        pause=0.1,
        timeout=30,
        session=None,
    ):

        super().__init__(
            api_key=api_key,
            retry_count=retry_count,
            pause=pause,
            timeout=timeout,
            session=session,
        )

        if api_key is None:
            api_key = os.getenv("GBIZINFO_API_KEY")
        if not api_key or not isinstance(api_key, str):
            raise ValueError(
                "The gBizINFO API key must be provided either "
                "through the api_key variable or through the "
                "environmental variable GBIZINFO_API_KEY."
            )

        self.api_key = api_key
        self.corporate_number = corporate_number

    @property
    def url(self):
        """API URL"""
        subsidy_URL = _BASE_URL + f"/{self.corporate_number}/subsidy?"
        return subsidy_URL

class workplaceReader(_gBizInfoReader):

    def __init__(
        self,
        api_key,
        corporate_number,
        retry_count=3,
        pause=0.1,
        timeout=30,
        session=None,
    ):

        super().__init__(
            api_key=api_key,
            retry_count=retry_count,
            pause=pause,
            timeout=timeout,
            session=session,
        )

        if api_key is None:
            api_key = os.getenv("GBIZINFO_API_KEY")
        if not api_key or not isinstance(api_key, str):
            raise ValueError(
                "The gBizINFO API key must be provided either "
                "through the api_key variable or through the "
                "environmental variable GBIZINFO_API_KEY."
            )

        self.api_key = api_key
        self.corporate_number = corporate_number

    @property
    def url(self):
        """API URL"""
        workplace_URL = _BASE_URL + f"/{self.corporate_number}/workplace?"
        return workplace_URL
