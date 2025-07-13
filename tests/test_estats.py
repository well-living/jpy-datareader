# test_estat.py
# -*- coding: utf-8 -*-

import json
import os
import pytest
import pandas as pd
import numpy as np
import requests
from requests.exceptions import RequestException, HTTPError, Timeout, ConnectionError
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path

# テスト対象のクラスをインポート
from jpy_datareader.estat import _eStatReader, StatsListReader, MetaInfoReader, StatsDataReader
from jpy_datareader._utils import RemoteDataError




class TestStatsListReader:
    """StatsListReaderクラスの単体テスト"""

    @pytest.fixture
    def mock_api_key(self):
        """テスト用のAPIキー"""
        return "test_api_key_12345"

    @pytest.fixture
    def expected_stats_list_response(self):
        """正常系のレスポンスデータ"""
        return {
            "GET_STATS_LIST": {
                "RESULT": {
                    "STATUS": 0,
                    "ERROR_MSG": "",
                    "DATE": "2025-01-15T10:30:00.000+09:00"
                },
                "PARAMETER": {
                    "LANG": "J",
                    "DATA_FORMAT": "json",
                    "LIMIT": 10000
                },
                "DATALIST_INF": {
                    "NUMBER": 3,
                    "RESULT_INF": {
                        "FROM_NUMBER": 1,
                        "TO_NUMBER": 3,
                        "NEXT_KEY": None
                    },
                    "TABLE_INF": [
                        {
                            "@id": "0000030001",
                            "STAT_NAME": {"@code": "00200521", "$": "国勢調査"},
                            "GOV_ORG": {"@code": "00200", "$": "総務省"},
                            "STATISTICS_NAME": "平成27年国勢調査",
                            "TITLE": "人口等基本集計（男女・年齢・配偶関係，世帯の構成，住居の状態など）",
                            "CYCLE": "",
                            "SURVEY_DATE": "201510",
                            "OPEN_DATE": "201610-201610",
                            "SMALL_AREA": 0,
                            "COLLECT_AREA": "全国",
                            "MAIN_CATEGORY": {"@code": "02", "$": "人口・世帯"},
                            "SUB_CATEGORY": {"@code": "01", "$": "人口"},
                            "OVERALL_TOTAL_NUMBER": 47790,
                            "UPDATED_DATE": "2016-10-26"
                        },
                        {
                            "@id": "0000030002",
                            "STAT_NAME": {"@code": "00200522", "$": "住宅・土地統計調査"},
                            "GOV_ORG": {"@code": "00200", "$": "総務省"},
                            "STATISTICS_NAME": "平成30年住宅・土地統計調査",
                            "TITLE": "住宅及び世帯に関する基本集計",
                            "CYCLE": "",
                            "SURVEY_DATE": "201810",
                            "OPEN_DATE": "201904-201904",
                            "SMALL_AREA": 0,
                            "COLLECT_AREA": "全国",
                            "MAIN_CATEGORY": {"@code": "08", "$": "住宅・土地・建設"},
                            "SUB_CATEGORY": {"@code": "01", "$": "住宅"},
                            "OVERALL_TOTAL_NUMBER": 15420,
                            "UPDATED_DATE": "2019-04-26"
                        }
                    ]
                }
            }
        }

    @pytest.fixture
    def expected_stats_list_dataframe(self):
        """期待されるDataFrame"""
        return pd.DataFrame([
            {
                "id": "0000030001",
                "STAT_NAME_code": "00200521",
                "STAT_NAME": "国勢調査",
                "GOV_ORG_code": "00200",
                "GOV_ORG": "総務省",
                "STATISTICS_NAME": "平成27年国勢調査",
                "TITLE": "人口等基本集計（男女・年齢・配偶関係，世帯の構成，住居の状態など）",
                "CYCLE": "",
                "SURVEY_DATE": "201510",
                "OPEN_DATE": "201610-201610",
                "SMALL_AREA": 0,
                "COLLECT_AREA": "全国",
                "MAIN_CATEGORY_code": "02",
                "MAIN_CATEGORY": "人口・世帯",
                "SUB_CATEGORY_code": "01",
                "SUB_CATEGORY": "人口",
                "OVERALL_TOTAL_NUMBER": 47790,
                "UPDATED_DATE": "2016-10-26"
            },
            {
                "id": "0000030002",
                "STAT_NAME_code": "00200522",
                "STAT_NAME": "住宅・土地統計調査",
                "GOV_ORG_code": "00200",
                "GOV_ORG": "総務省",
                "STATISTICS_NAME": "平成30年住宅・土地統計調査",
                "TITLE": "住宅及び世帯に関する基本集計",
                "CYCLE": "",
                "SURVEY_DATE": "201810",
                "OPEN_DATE": "201904-201904",
                "SMALL_AREA": 0,
                "COLLECT_AREA": "全国",
                "MAIN_CATEGORY_code": "08",
                "MAIN_CATEGORY": "住宅・土地・建設",
                "SUB_CATEGORY_code": "01",
                "SUB_CATEGORY": "住宅",
                "OVERALL_TOTAL_NUMBER": 15420,
                "UPDATED_DATE": "2019-04-26"
            }
        ])

    def test_init_with_api_key(self, mock_api_key):
        """APIキーを指定した初期化のテスト"""
        reader = StatsListReader(api_key=mock_api_key)
        assert reader.api_key == mock_api_key
        assert reader.lang is None
        assert reader.explanationGetFlg is None

    def test_init_with_parameters(self, mock_api_key):
        """パラメータを指定した初期化のテスト"""
        reader = StatsListReader(
            api_key=mock_api_key,
            lang="E",
            explanationGetFlg="Y",
            searchWord="人口",
            searchKind=1,
            collectArea=1
        )
        assert reader.api_key == mock_api_key
        assert reader.lang == "E"
        assert reader.explanationGetFlg == "Y"
        assert reader.searchWord == "人口"
        assert reader.searchKind == 1
        assert reader.collectArea == 1

    def test_init_without_api_key_raises_error(self):
        """APIキーなしの初期化で例外が発生することをテスト"""
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(ValueError, match="The e-Stat Application ID must be provided"):
                StatsListReader()

    def test_url_property(self, mock_api_key):
        """URLプロパティのテスト"""
        reader = StatsListReader(api_key=mock_api_key)
        expected_url = "https://api.e-stat.go.jp/rest/3.0/app/json/getStatsList?"
        assert reader.url == expected_url

    def test_params_property_basic(self, mock_api_key):
        """基本的なparamsプロパティのテスト"""
        reader = StatsListReader(api_key=mock_api_key)
        params = reader.params
        assert params["appId"] == mock_api_key
        assert len(params) == 1

    def test_params_property_with_filters(self, mock_api_key):
        """フィルタ付きparamsプロパティのテスト"""
        reader = StatsListReader(
            api_key=mock_api_key,
            searchWord="人口",
            searchKind=1,
            collectArea=2,
            explanationGetFlg="Y"
        )
        params = reader.params
        assert params["appId"] == mock_api_key
        assert params["searchWord"] == "人口"
        assert params["searchKind"] == 1
        assert params["collectArea"] == 2
        assert params["explanationGetFlg"] == "Y"

    @patch('jpy_datareader.estat._BaseReader._get_response')
    def test_read_success(self, mock_get_response, mock_api_key, expected_stats_list_response):
        """正常系: データ取得成功のテスト"""
        # モックレスポンスの設定
        mock_response = Mock()
        mock_response.json.return_value = expected_stats_list_response
        mock_get_response.return_value = mock_response

        reader = StatsListReader(api_key=mock_api_key)
        result = reader.read()

        # レスポンス取得が呼ばれたことを確認
        mock_get_response.assert_called_once()
        
        # 結果がDataFrameであることを確認
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert "id" in result.columns
        assert result.iloc[0]["id"] == "0000030001"
        assert result.iloc[1]["id"] == "0000030002"

        # インスタンス変数に結果が保存されていることを確認
        assert reader.STATUS == 0
        assert reader.NUMBER == 3

    @patch('jpy_datareader.estat._BaseReader._get_response')
    def test_read_json_success(self, mock_get_response, mock_api_key, expected_stats_list_response):
        """正常系: JSON取得成功のテスト"""
        # モックレスポンスの設定
        mock_response = Mock()
        mock_response.json.return_value = expected_stats_list_response
        mock_get_response.return_value = mock_response

        reader = StatsListReader(api_key=mock_api_key)
        result = reader.read_json()

        # レスポンス取得が呼ばれたことを確認
        mock_get_response.assert_called_once()
        
        # 結果が期待されるJSONと一致することを確認
        assert result == expected_stats_list_response
        
        # インスタンス変数に結果が保存されていることを確認
        assert reader.STATUS == 0
        assert reader.NUMBER == 3

    @patch('jpy_datareader.estat._BaseReader._get_response')
    def test_read_api_error_status(self, mock_get_response, mock_api_key):
        """異常系: API側でエラーステータスが返されるテスト"""
        error_response = {
            "GET_STATS_LIST": {
                "RESULT": {
                    "STATUS": 1,
                    "ERROR_MSG": "統計表IDが存在しません。",
                    "DATE": "2025-01-15T10:30:00.000+09:00"
                }
            }
        }
        
        mock_response = Mock()
        mock_response.json.return_value = error_response
        mock_get_response.return_value = mock_response

        reader = StatsListReader(api_key=mock_api_key)
        result = reader.read()

        # エラーステータスでも結果は返される（空のDataFrame）
        assert isinstance(result, pd.DataFrame)
        assert reader.STATUS == 1
        assert reader.ERROR_MSG == "統計表IDが存在しません。"

    @patch('jpy_datareader.estat._BaseReader._get_response')
    def test_read_missing_datalist_inf(self, mock_get_response, mock_api_key):
        """異常系: DATALIST_INFが存在しない場合のテスト"""
        incomplete_response = {
            "GET_STATS_LIST": {
                "RESULT": {
                    "STATUS": 0,
                    "ERROR_MSG": "",
                    "DATE": "2025-01-15T10:30:00.000+09:00"
                }
                # DATALIST_INFが存在しない
            }
        }
        
        mock_response = Mock()
        mock_response.json.return_value = incomplete_response
        mock_get_response.return_value = mock_response

        reader = StatsListReader(api_key=mock_api_key)
        
        # pandas.json_normalizeが空のrecord_pathで例外を発生させることを期待
        with pytest.raises((ValueError, KeyError)):
            reader.read()

    @patch('jpy_datareader.estat._BaseReader._get_response')
    def test_read_communication_error(self, mock_get_response, mock_api_key):
        """異常系: 通信例外のテスト"""
        mock_get_response.side_effect = RemoteDataError("Unable to read URL")

        reader = StatsListReader(api_key=mock_api_key)
        
        with pytest.raises(RemoteDataError, match="Unable to read URL"):
            reader.read()

    def test_data_consistency_with_local_file(self, mock_api_key, tmp_path):
        """整合性テスト: ローカルファイルとの比較"""
        # テストデータを一時ファイルに保存
        test_data = {
            "GET_STATS_LIST": {
                "RESULT": {"STATUS": 0, "ERROR_MSG": "", "DATE": "2025-01-15T10:30:00.000+09:00"},
                "PARAMETER": {"LANG": "J", "DATA_FORMAT": "json", "LIMIT": 10000},
                "DATALIST_INF": {
                    "NUMBER": 1,
                    "RESULT_INF": {"FROM_NUMBER": 1, "TO_NUMBER": 1, "NEXT_KEY": None},
                    "TABLE_INF": [{
                        "@id": "test001",
                        "STAT_NAME": {"@code": "00001", "$": "テスト統計"},
                        "GOV_ORG": {"@code": "00100", "$": "テスト省"},
                        "STATISTICS_NAME": "テスト調査",
                        "TITLE": "テスト集計",
                        "OVERALL_TOTAL_NUMBER": 100
                    }]
                }
            }
        }
        
        # テストデータファイルを作成
        test_file = tmp_path / "expected_data.json"
        with open(test_file, 'w', encoding='utf-8') as f:
            json.dump(test_data, f, ensure_ascii=False, indent=2)

        # ファイルからデータを読み込み
        with open(test_file, 'r', encoding='utf-8') as f:
            expected_data = json.load(f)

        with patch('jpy_datareader.estat._BaseReader._get_response') as mock_get_response:
            mock_response = Mock()
            mock_response.json.return_value = test_data
            mock_get_response.return_value = mock_response

            reader = StatsListReader(api_key=mock_api_key)
            result_json = reader.read_json()

            # レスポンスデータとファイルデータが一致することを確認
            assert result_json == expected_data
            
            # 重要なキーが存在することを確認
            table_inf = result_json["GET_STATS_LIST"]["DATALIST_INF"]["TABLE_INF"][0]
            assert table_inf["@id"] == "test001"
            assert table_inf["STAT_NAME"]["$"] == "テスト統計"


class TestMetaInfoReader:
    """MetaInfoReaderクラスの単体テスト"""

    @pytest.fixture
    def mock_api_key(self):
        """テスト用のAPIキー"""
        return "test_api_key_12345"

    @pytest.fixture
    def mock_stats_data_id(self):
        """テスト用の統計データID"""
        return "0000030001"

    @pytest.fixture
    def expected_metainfo_response(self):
        """正常系のレスポンスデータ"""
        return {
            "GET_META_INFO": {
                "RESULT": {
                    "STATUS": 0,
                    "ERROR_MSG": "",
                    "DATE": "2025-01-15T10:30:00.000+09:00"
                },
                "PARAMETER": {
                    "LANG": "J",
                    "DATA_FORMAT": "json"
                },
                "METADATA_INF": {
                    "TABLE_INF": {
                        "STAT_NAME": "国勢調査",
                        "GOV_ORG": "総務省",
                        "STATISTICS_NAME": "平成27年国勢調査",
                        "TITLE": "人口等基本集計"
                    },
                    "CLASS_INF": {
                        "CLASS_OBJ": [
                            {
                                "@id": "tab",
                                "@name": "表章項目",
                                "CLASS": [
                                    {
                                        "@code": "001",
                                        "@name": "人口",
                                        "@level": "1",
                                        "@unit": "人"
                                    },
                                    {
                                        "@code": "002", 
                                        "@name": "世帯数",
                                        "@level": "1",
                                        "@unit": "世帯"
                                    }
                                ]
                            },
                            {
                                "@id": "cat01",
                                "@name": "男女",
                                "CLASS": [
                                    {
                                        "@code": "001",
                                        "@name": "総数",
                                        "@level": "1"
                                    },
                                    {
                                        "@code": "002",
                                        "@name": "男",
                                        "@level": "1"
                                    },
                                    {
                                        "@code": "003",
                                        "@name": "女",
                                        "@level": "1"
                                    }
                                ]
                            },
                            {
                                "@id": "time",
                                "@name": "時間軸",
                                "CLASS": [
                                    {
                                        "@code": "2015000000",
                                        "@name": "2015年",
                                        "@level": "1"
                                    }
                                ]
                            }
                        ]
                    }
                }
            }
        }

    def test_init_with_required_params(self, mock_api_key, mock_stats_data_id):
        """必須パラメータでの初期化テスト"""
        reader = MetaInfoReader(api_key=mock_api_key, statsDataId=mock_stats_data_id)
        assert reader.api_key == mock_api_key
        assert reader.statsDataId == mock_stats_data_id
        assert reader.prefix_colname_with_classname is True
        assert reader.has_lv_hierarchy is False

    def test_init_with_optional_params(self, mock_api_key, mock_stats_data_id):
        """オプションパラメータでの初期化テスト"""
        reader = MetaInfoReader(
            api_key=mock_api_key,
            statsDataId=mock_stats_data_id,
            prefix_colname_with_classname=False,
            has_lv_hierarchy=True,
            use_fillna_lv_hierarchy=False,
            lang="E",
            explanationGetFlg="N"
        )
        assert reader.prefix_colname_with_classname is False
        assert reader.has_lv_hierarchy is True
        assert reader.use_fillna_lv_hierarchy is False
        assert reader.lang == "E"
        assert reader.explanationGetFlg == "N"

    def test_url_property(self, mock_api_key, mock_stats_data_id):
        """URLプロパティのテスト"""
        reader = MetaInfoReader(api_key=mock_api_key, statsDataId=mock_stats_data_id)
        expected_url = "https://api.e-stat.go.jp/rest/3.0/app/json/getMetaInfo?"
        assert reader.url == expected_url

    def test_params_property(self, mock_api_key, mock_stats_data_id):
        """paramsプロパティのテスト"""
        reader = MetaInfoReader(
            api_key=mock_api_key,
            statsDataId=mock_stats_data_id,
            explanationGetFlg="Y"
        )
        params = reader.params
        assert params["appId"] == mock_api_key
        assert params["statsDataId"] == mock_stats_data_id
        assert params["explanationGetFlg"] == "Y"

    @patch('jpy_datareader.estat._BaseReader._get_response')
    def test_read_success(self, mock_get_response, mock_api_key, mock_stats_data_id, expected_metainfo_response):
        """正常系: データ取得成功のテスト"""
        mock_response = Mock()
        mock_response.json.return_value = expected_metainfo_response
        mock_get_response.return_value = mock_response

        reader = MetaInfoReader(api_key=mock_api_key, statsDataId=mock_stats_data_id)
        result = reader.read()

        # レスポンス取得が呼ばれたことを確認
        mock_get_response.assert_called_once()
        
        # 結果がDataFrameであることを確認
        assert isinstance(result, pd.DataFrame)
        
        # 最も多い行数のDataFrameが返されることを確認（timeを除く）
        # この場合、男女が3行で最大
        assert len(result) == 3
        assert "男女code" in result.columns or "code" in result.columns

        # インスタンス変数に結果が保存されていることを確認
        assert reader.STATUS == 0
        assert reader.STAT_NAME == "国勢調査"

    @patch('jpy_datareader.estat._BaseReader._get_response')
    def test_read_class_objs_success(self, mock_get_response, mock_api_key, mock_stats_data_id, expected_metainfo_response):
        """正常系: クラスオブジェクト取得成功のテスト"""
        mock_response = Mock()
        mock_response.json.return_value = expected_metainfo_response
        mock_get_response.return_value = mock_response

        reader = MetaInfoReader(api_key=mock_api_key, statsDataId=mock_stats_data_id)
        result = reader.read_class_objs()

        # 結果がリストであることを確認
        assert isinstance(result, list)
        assert len(result) == 3  # tab, cat01, time

        # 各要素の構造を確認
        for class_data in result:
            assert "id" in class_data
            assert "name" in class_data
            assert "meta_dataframe" in class_data
            assert isinstance(class_data["meta_dataframe"], pd.DataFrame)

        # 特定のクラスIDを確認
        ids = [class_data["id"] for class_data in result]
        assert "tab" in ids
        assert "cat01" in ids
        assert "time" in ids

    @patch('jpy_datareader.estat._BaseReader._get_response')
    def test_read_json_success(self, mock_get_response, mock_api_key, mock_stats_data_id, expected_metainfo_response):
        """正常系: JSON取得成功のテスト"""
        mock_response = Mock()
        mock_response.json.return_value = expected_metainfo_response
        mock_get_response.return_value = mock_response

        reader = MetaInfoReader(api_key=mock_api_key, statsDataId=mock_stats_data_id)
        result = reader.read_json()

        # レスポンス取得が呼ばれたことを確認
        mock_get_response.assert_called_once()
        
        # 結果が期待されるJSONと一致することを確認
        assert result == expected_metainfo_response

    @patch('jpy_datareader.estat._BaseReader._get_response')
    def test_read_api_error_status(self, mock_get_response, mock_api_key, mock_stats_data_id):
        """異常系: API側でエラーステータスが返されるテスト"""
        error_response = {
            "GET_META_INFO": {
                "RESULT": {
                    "STATUS": 1,
                    "ERROR_MSG": "統計表IDが存在しません。",
                    "DATE": "2025-01-15T10:30:00.000+09:00"
                }
            }
        }
        
        mock_response = Mock()
        mock_response.json.return_value = error_response
        mock_get_response.return_value = mock_response

        reader = MetaInfoReader(api_key=mock_api_key, statsDataId=mock_stats_data_id)
        result = reader.read_class_objs()

        # エラーステータスでも結果は返される（空のリスト）
        assert isinstance(result, list)
        assert len(result) == 0
        assert reader.STATUS == 1
        assert reader.ERROR_MSG == "統計表IDが存在しません。"

    @patch('jpy_datareader.estat._BaseReader._get_response')
    def test_read_missing_class_inf(self, mock_get_response, mock_api_key, mock_stats_data_id):
        """異常系: CLASS_INFが存在しない場合のテスト"""
        incomplete_response = {
            "GET_META_INFO": {
                "RESULT": {
                    "STATUS": 0,
                    "ERROR_MSG": "",
                    "DATE": "2025-01-15T10:30:00.000+09:00"
                },
                "METADATA_INF": {
                    "TABLE_INF": {
                        "STAT_NAME": "国勢調査"
                    }
                    # CLASS_INFが存在しない
                }
            }
        }
        
        mock_response = Mock()
        mock_response.json.return_value = incomplete_response
        mock_get_response.return_value = mock_response

        reader = MetaInfoReader(api_key=mock_api_key, statsDataId=mock_stats_data_id)
        result = reader.read_class_objs()

        # CLASS_INFが存在しない場合は空のリストが返される
        assert isinstance(result, list)
        assert len(result) == 0

    @patch('jpy_datareader.estat._BaseReader._get_response')
    def test_read_communication_error(self, mock_get_response, mock_api_key, mock_stats_data_id):
        """異常系: 通信例外のテスト"""
        mock_get_response.side_effect = RemoteDataError("Unable to read URL")

        reader = MetaInfoReader(api_key=mock_api_key, statsDataId=mock_stats_data_id)
        
        with pytest.raises(RemoteDataError, match="Unable to read URL"):
            reader.read()

    def test_create_class_dataframe_with_list(self, mock_api_key, mock_stats_data_id):
        """_create_class_dataframeメソッドのリスト入力テスト"""
        reader = MetaInfoReader(api_key=mock_api_key, statsDataId=mock_stats_data_id)
        
        class_data = [
            {"@code": "001", "@name": "テスト1", "@level": "1"},
            {"@code": "002", "@name": "テスト2", "@level": "2"}
        ]
        
        result = reader._create_class_dataframe(class_data)
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert "@code" in result.columns
        assert "@name" in result.columns
        assert "@level" in result.columns

    def test_create_class_dataframe_with_dict(self, mock_api_key, mock_stats_data_id):
        """_create_class_dataframeメソッドの辞書入力テスト"""
        reader = MetaInfoReader(api_key=mock_api_key, statsDataId=mock_stats_data_id)
        
        class_data = {"@code": "001", "@name": "テスト", "@level": "1"}
        
        result = reader._create_class_dataframe(class_data)
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        assert result.iloc[0]["@code"] == "001"

    def test_data_consistency_with_local_file(self, mock_api_key, mock_stats_data_id, tmp_path):
        """整合性テスト: ローカルファイルとの比較"""
        # テストデータを一時ファイルに保存
        test_data = {
            "GET_META_INFO": {
                "RESULT": {"STATUS": 0, "ERROR_MSG": "", "DATE": "2025-01-15T10:30:00.000+09:00"},
                "PARAMETER": {"LANG": "J", "DATA_FORMAT": "json"},
                "METADATA_INF": {
                    "TABLE_INF": {"STAT_NAME": "テスト統計", "GOV_ORG": "テスト省"},
                    "CLASS_INF": {
                        "CLASS_OBJ": [{
                            "@id": "test_tab",
                            "@name": "テスト項目",
                            "CLASS": [
                                {"@code": "001", "@name": "項目1", "@level": "1"},
                                {"@code": "002", "@name": "項目2", "@level": "1"}
                            ]
                        }]
                    }
                }
            }
        }
        
        # テストデータファイルを作成
        test_file = tmp_path / "expected_metainfo.json"
        with open(test_file, 'w', encoding='utf-8') as f:
            json.dump(test_data, f, ensure_ascii=False, indent=2)

        # ファイルからデータを読み込み
        with open(test_file, 'r', encoding='utf-8') as f:
            expected_data = json.load(f)

        with patch('jpy_datareader.estat._BaseReader._get_response') as mock_get_response:
            mock_response = Mock()
            mock_response.json.return_value = test_data
            mock_get_response.return_value = mock_response

            reader = MetaInfoReader(api_key=mock_api_key, statsDataId=mock_stats_data_id)
            result_json = reader.read_json()

            # レスポンスデータとファイルデータが一致することを確認
            assert result_json == expected_data
            
            # 重要なキーが存在することを確認
            class_obj = result_json["GET_META_INFO"]["METADATA_INF"]["CLASS_INF"]["CLASS_OBJ"][0]
            assert class_obj["@id"] == "test_tab"
            assert class_obj["@name"] == "テスト項目"
            assert len(class_obj["CLASS"]) == 2

    def test_prefix_colname_functionality(self, mock_api_key, mock_stats_data_id):
        """列名プレフィックス機能のテスト"""
        # プレフィックスありの場合
        reader_with_prefix = MetaInfoReader(
            api_key=mock_api_key,
            statsDataId=mock_stats_data_id,
            prefix_colname_with_classname=True
        )
        
        class_data = [{"@code": "001", "@name": "テスト"}]
        result_with_prefix = reader_with_prefix._apply_colname_transformations(
            pd.DataFrame(class_data), "テスト項目"
        )
        
        # プレフィックスなしの場合
        reader_without_prefix = MetaInfoReader(
            api_key=mock_api_key,
            statsDataId=mock_stats_data_id,
            prefix_colname_with_classname=False
        )
        
        result_without_prefix = reader_without_prefix._apply_colname_transformations(
            pd.DataFrame(class_data), "テスト項目"
        )
        
        # プレフィックスありの場合は列名にクラス名が含まれる
        assert any("テスト項目" in col for col in result_with_prefix.columns)
        
        # プレフィックスなしの場合は列名にクラス名が含まれない（ただし日本語変換は適用される）
        prefix_cols = [col for col in result_without_prefix.columns if "テスト項目" in col]
        assert len(prefix_cols) <= 1  # 日本語変換で1つだけクラス名列が追加される可能性


class TestErrorHandling:
    """エラーハンドリングの統合テスト"""

    @pytest.fixture
    def mock_api_key(self):
        return "test_api_key"

    @patch('jpy_datareader.estat._BaseReader._get_response')
    def test_http_404_error(self, mock_get_response, mock_api_key):
        """HTTP 404エラーのテスト"""
        mock_get_response.side_effect = RemoteDataError("Unable to read URL: HTTP 404")

        reader = StatsListReader(api_key=mock_api_key)
        
        with pytest.raises(RemoteDataError, match="HTTP 404"):
            reader.read()

    @patch('jpy_datareader.estat._BaseReader._get_response')
    def test_http_500_error(self, mock_get_response, mock_api_key):
        """HTTP 500エラーのテスト"""
        mock_get_response.side_effect = RemoteDataError("Unable to read URL: HTTP 500")

        reader = MetaInfoReader(api_key=mock_api_key, statsDataId="test_id")
        
        with pytest.raises(RemoteDataError, match="HTTP 500"):
            reader.read()

    @patch('jpy_datareader.estat._BaseReader._get_response')
    def test_timeout_error(self, mock_get_response, mock_api_key):
        """タイムアウトエラーのテスト"""
        mock_get_response.side_effect = RemoteDataError("Timeout occurred")

        reader = StatsListReader(api_key=mock_api_key, timeout=1)
        
        with pytest.raises(RemoteDataError, match="Timeout occurred"):
            reader.read()

    @patch('jpy_datareader.estat._BaseReader._get_response')
    def test_malformed_json_response(self, mock_get_response, mock_api_key):
        """不正なJSONレスポンスのテスト"""
        mock_response = Mock()
        mock_response.json.side_effect = ValueError("Invalid JSON")
        mock_get_response.return_value = mock_response

        reader = StatsListReader(api_key=mock_api_key)
        
        with pytest.raises(ValueError, match="Invalid JSON"):
            reader.read()


class TestEnvironmentVariables:
    """環境変数関連のテスト"""

    def test_api_key_from_env_estat_application_id(self):
        """E_STAT_APPLICATION_ID環境変数からAPIキーを取得するテスト"""
        with patch.dict(os.environ, {"E_STAT_APPLICATION_ID": "env_test_key"}, clear=True):
            reader = StatsListReader()
            assert reader.api_key == "env_test_key"

    def test_api_key_from_env_estat_api_key(self):
        """ESTAT_API_KEY環境変数からAPIキーを取得するテスト"""
        with patch.dict(os.environ, {"ESTAT_API_KEY": "env_api_key"}, clear=True):
            reader = StatsListReader()
            assert reader.api_key == "env_api_key"

    def test_api_key_priority_order(self):
        """環境変数の優先順位テスト"""
        with patch.dict(os.environ, {
            "E_STAT_APPLICATION_ID": "first_priority",
            "ESTAT_APPLICATION_ID": "second_priority",
            "E_STAT_API_KEY": "lower_priority"
        }, clear=True):
            reader = StatsListReader()
            # E_STAT_APPLICATION_IDが最優先
            assert reader.api_key == "first_priority"

    def test_api_key_explicit_overrides_env(self):
        """明示的なAPIキーが環境変数より優先されることをテスト"""
        with patch.dict(os.environ, {"E_STAT_APPLICATION_ID": "env_key"}, clear=True):
            reader = StatsListReader(api_key="explicit_key")
            assert reader.api_key == "explicit_key"


class TestParameterValidation:
    """パラメータ検証のテスト"""

    @pytest.fixture
    def mock_api_key(self):
        return "test_key"

    def test_stats_list_reader_invalid_search_kind(self, mock_api_key):
        """StatsListReaderの無効なsearchKindパラメータテスト"""
        reader = StatsListReader(api_key=mock_api_key, searchKind=5)  # 無効な値
        params = reader.params
        
        # 無効な値は含まれない
        assert "searchKind" not in params

    def test_stats_list_reader_valid_search_kind(self, mock_api_key):
        """StatsListReaderの有効なsearchKindパラメータテスト"""
        reader = StatsListReader(api_key=mock_api_key, searchKind=1)  # 有効な値
        params = reader.params
        
        # 有効な値は含まれる
        assert params["searchKind"] == 1

    def test_stats_list_reader_invalid_collect_area(self, mock_api_key):
        """StatsListReaderの無効なcollectAreaパラメータテスト"""
        reader = StatsListReader(api_key=mock_api_key, collectArea=5)  # 無効な値
        params = reader.params
        
        # 無効な値は含まれない
        assert "collectArea" not in params

    def test_stats_list_reader_valid_collect_area(self, mock_api_key):
        """StatsListReaderの有効なcollectAreaパラメータテスト"""
        for valid_area in [1, 2, 3]:
            reader = StatsListReader(api_key=mock_api_key, collectArea=valid_area)
            params = reader.params
            assert params["collectArea"] == valid_area

    def test_metainfo_reader_invalid_explanation_flag(self, mock_api_key):
        """MetaInfoReaderの無効なexplanationGetFlgパラメータテスト"""
        reader = MetaInfoReader(
            api_key=mock_api_key,
            statsDataId="test_id",
            explanationGetFlg="X"  # 無効な値
        )
        params = reader.params
        
        # 無効な値は含まれない
        assert "explanationGetFlg" not in params

    def test_metainfo_reader_valid_explanation_flag(self, mock_api_key):
        """MetaInfoReaderの有効なexplanationGetFlgパラメータテスト"""
        for valid_flag in ["Y", "N"]:
            reader = MetaInfoReader(
                api_key=mock_api_key,
                statsDataId="test_id",
                explanationGetFlg=valid_flag
            )
            params = reader.params
            assert params["explanationGetFlg"] == valid_flag


class TestDataIntegrity:
    """データ整合性の統合テスト"""

    @pytest.fixture
    def sample_csv_data(self, tmp_path):
        """サンプルCSVデータの作成"""
        csv_content = """id,stat_name,gov_org,title,overall_total_number
0000030001,国勢調査,総務省,人口等基本集計,47790
0000030002,住宅・土地統計調査,総務省,住宅及び世帯に関する基本集計,15420"""
        
        csv_file = tmp_path / "expected_stats_list.csv"
        with open(csv_file, 'w', encoding='utf-8') as f:
            f.write(csv_content)
        
        return csv_file

    @patch('jpy_datareader.estat._BaseReader._get_response')
    def test_compare_with_csv_data(self, mock_get_response, sample_csv_data):
        """CSVファイルとの比較テスト"""
        # API レスポンスを作成
        api_response = {
            "GET_STATS_LIST": {
                "RESULT": {"STATUS": 0, "ERROR_MSG": "", "DATE": "2025-01-15T10:30:00.000+09:00"},
                "PARAMETER": {"LANG": "J", "DATA_FORMAT": "json", "LIMIT": 10000},
                "DATALIST_INF": {
                    "NUMBER": 2,
                    "RESULT_INF": {"FROM_NUMBER": 1, "TO_NUMBER": 2, "NEXT_KEY": None},
                    "TABLE_INF": [
                        {
                            "@id": "0000030001",
                            "STAT_NAME": {"@code": "00200521", "$": "国勢調査"},
                            "GOV_ORG": {"@code": "00200", "$": "総務省"},
                            "STATISTICS_NAME": "平成27年国勢調査",
                            "TITLE": "人口等基本集計（男女・年齢・配偶関係，世帯の構成，住居の状態など）",
                            "OVERALL_TOTAL_NUMBER": 47790,
                            "UPDATED_DATE": "2016-10-26"
                        },
                        {
                            "@id": "0000030002",
                            "STAT_NAME": {"@code": "00200522", "$": "住宅・土地統計調査"},
                            "GOV_ORG": {"@code": "00200", "$": "総務省"},
                            "STATISTICS_NAME": "平成30年住宅・土地統計調査",
                            "TITLE": "住宅及び世帯に関する基本集計",
                            "OVERALL_TOTAL_NUMBER": 15420,
                            "UPDATED_DATE": "2019-04-26"
                        }
                    ]
                }
            }
        }

        mock_response = Mock()
        mock_response.json.return_value = api_response
        mock_get_response.return_value = mock_response

        # APIからデータを取得
        reader = StatsListReader(api_key="test_key")
        api_result = reader.read()

        # CSVファイルからデータを読み込み
        expected_df = pd.read_csv(sample_csv_data)

        # データの整合性を確認
        assert len(api_result) == len(expected_df)
        
        # IDの一致を確認
        api_ids = set(api_result["id"].tolist())
        expected_ids = set(expected_df["id"].tolist())
        assert api_ids == expected_ids

        # 特定の値の確認
        api_first_row = api_result[api_result["id"] == "0000030001"].iloc[0]
        expected_first_row = expected_df[expected_df["id"] == "0000030001"].iloc[0]
        
        assert api_first_row["OVERALL_TOTAL_NUMBER"] == expected_first_row["overall_total_number"]

    def test_type_consistency(self):
        """データ型の一貫性テスト"""
        # 文字列のIDが数値として解釈されないことを確認
        test_data = {
            "GET_STATS_LIST": {
                "RESULT": {"STATUS": 0, "ERROR_MSG": "", "DATE": "2025-01-15T10:30:00.000+09:00"},
                "PARAMETER": {"LANG": "J", "DATA_FORMAT": "json", "LIMIT": 10000},
                "DATALIST_INF": {
                    "NUMBER": 1,
                    "RESULT_INF": {"FROM_NUMBER": 1, "TO_NUMBER": 1, "NEXT_KEY": None},
                    "TABLE_INF": [{
                        "@id": "0000030001",  # 文字列のID
                        "STAT_NAME": {"@code": "00001", "$": "テスト統計"},
                        "OVERALL_TOTAL_NUMBER": 12345  # 数値
                    }]
                }
            }
        }

        with patch('jpy_datareader.estat._BaseReader._get_response') as mock_get_response:
            mock_response = Mock()
            mock_response.json.return_value = test_data
            mock_get_response.return_value = mock_response

            reader = StatsListReader(api_key="test_key")
            result = reader.read()

            # IDは文字列として保持される
            assert isinstance(result.iloc[0]["id"], str)
            assert result.iloc[0]["id"] == "0000030001"
            
            # 数値は適切な型で保持される
            assert isinstance(result.iloc[0]["OVERALL_TOTAL_NUMBER"], (int, float))
            assert result.iloc[0]["OVERALL_TOTAL_NUMBER"] == 12345


# 実際のcolname_to_japanese関数をインポート
from jpy_datareader.estat import colname_to_japanese


class TestColumnNameTranslation:
    """列名の日本語変換機能のテスト"""

    def test_colname_to_japanese_function(self):
        """colname_to_japanese関数の動作テスト"""
        # テスト用のDataFrame
        test_df = pd.DataFrame({
            "code": ["001", "002"],
            "name": ["テスト1", "テスト2"], 
            "value": [100, 200],
            "level": [1, 2],
            "unit": ["人", "世帯"],
            "other_col": ["その他1", "その他2"]
        })
        
        result = colname_to_japanese(test_df)
        
        # 変換結果を確認
        expected_columns = ["コード", "", "値", "階層レベル", "単位", "other_col"]
        assert list(result.columns) == expected_columns
        
        # データは変更されていないことを確認
        assert result["コード"].tolist() == ["001", "002"]
        assert result["値"].tolist() == [100, 200]

    def test_colname_to_japanese_with_prefix(self):
        """プレフィックス付き列名の日本語変換テスト"""
        test_df = pd.DataFrame({
            "categorycode": ["A", "B"],
            "categoryname": ["カテゴリA", "カテゴリB"],
            "tabvalue": [10, 20]
        })
        
        result = colname_to_japanese(test_df)
        
        # 部分一致での変換を確認
        expected_columns = ["categoryコード", "category", "tab値"]
        assert list(result.columns) == expected_columns

    def test_colname_to_japanese_no_match(self):
        """変換対象がない列名のテスト"""
        test_df = pd.DataFrame({
            "custom_col1": [1, 2],
            "custom_col2": [3, 4],
            "日本語列": [5, 6]
        })
        
        result = colname_to_japanese(test_df)
        
        # 変換対象がない場合は元の列名のまま
        expected_columns = ["custom_col1", "custom_col2", "日本語列"]
        assert list(result.columns) == expected_columns



class TestStatsDataReader:
    """Test suite for StatsDataReader class."""
    
    @pytest.fixture
    def mock_api_key(self):
        """Fixture providing a mock API key."""
        return "test_api_key_12345"
    
    @pytest.fixture
    def mock_stats_data_id(self):
        """Fixture providing a mock statistics data ID."""
        return "0003348423"
    
    @pytest.fixture
    def stats_reader(self, mock_api_key, mock_stats_data_id):
        """Fixture providing a StatsDataReader instance."""
        return StatsDataReader(
            api_key=mock_api_key,
            statsDataId=mock_stats_data_id,
            retry_count=1,  # Reduce retry for faster tests
            timeout=5
        )
    
    @pytest.fixture
    def expected_data_json(self):
        """Fixture providing expected JSON response data."""
        return {
            "GET_STATS_DATA": {
                "RESULT": {
                    "STATUS": 0,
                    "ERROR_MSG": "",
                    "DATE": "2025-07-13T10:30:00.000+09:00"
                },
                "PARAMETER": {
                    "LANG": "J",
                    "DATA_FORMAT": "JSON",
                    "START_POSITION": "1",
                    "METAGET_FLG": "Y"
                },
                "STATISTICAL_DATA": {
                    "RESULT_INF": {
                        "TOTAL_NUMBER": 4,
                        "FROM_NUMBER": 1,
                        "TO_NUMBER": 4
                    },
                    "TABLE_INF": {
                        "STATISTICS_NAME": "人口推計",
                        "TITLE": "人口推計（月報）",
                        "CYCLE": "月次",
                        "OVERALL_TOTAL_NUMBER": 4,
                        "GOV_ORG": "総務省"
                    },
                    "CLASS_INF": {
                        "CLASS_OBJ": [
                            {
                                "@id": "tab",
                                "@name": "表章項目",
                                "CLASS": [
                                    {"@code": "01", "@name": "総人口", "@level": "1", "@unit": "千人"},
                                    {"@code": "02", "@name": "男性人口", "@level": "1", "@unit": "千人"}
                                ]
                            },
                            {
                                "@id": "area",
                                "@name": "地域",
                                "CLASS": [
                                    {"@code": "00000", "@name": "全国", "@level": "1"},
                                    {"@code": "01000", "@name": "北海道", "@level": "2"}
                                ]
                            },
                            {
                                "@id": "time",
                                "@name": "時間軸",
                                "CLASS": [
                                    {"@code": "2024001000", "@name": "2024年1月", "@level": "1"},
                                    {"@code": "2024002000", "@name": "2024年2月", "@level": "1"}
                                ]
                            }
                        ]
                    },
                    "DATA_INF": {
                        "VALUE": [
                            {"@tab": "01", "@area": "00000", "@time": "2024001000", "$": "125000"},
                            {"@tab": "01", "@area": "00000", "@time": "2024002000", "$": "124950"},
                            {"@tab": "02", "@area": "01000", "@time": "2024001000", "$": "2500"},
                            {"@tab": "02", "@area": "01000", "@time": "2024002000", "$": "2498"}
                        ]
                    }
                }
            }
        }
    
    @pytest.fixture
    def expected_dataframe(self):
        """Fixture providing expected DataFrame result."""
        return pd.DataFrame({
            'tab_code': ['01', '01', '02', '02'],
            'area_code': ['00000', '00000', '01000', '01000'],
            'time_code': ['2024001000', '2024002000', '2024001000', '2024002000'],
            'value': [125000.0, 124950.0, 2500.0, 2498.0],
            'tab_name': ['総人口', '総人口', '男性人口', '男性人口'],
            'tab_level': ['1', '1', '1', '1'],
            'tab_unit': ['千人', '千人', '千人', '千人'],
            'area_name': ['全国', '全国', '北海道', '北海道'],
            'area_level': ['1', '1', '2', '2'],
            'time_name': ['2024年1月', '2024年2月', '2024年1月', '2024年2月'],
            'time_level': ['1', '1', '1', '1']
        })

    # === 正常系テスト ===
    
    @patch('jpy_datareader.base._BaseReader._get_response')
    def test_read_json_success(self, mock_get_response, stats_reader, expected_data_json):
        """Test successful JSON data retrieval."""
        # Setup mock response
        mock_response = Mock()
        mock_response.json.return_value = expected_data_json
        mock_get_response.return_value = mock_response
        
        # Execute
        result = stats_reader.read_json()
        
        # Verify
        assert result == expected_data_json
        assert stats_reader.STATUS == 0
        assert stats_reader.STATISTICS_NAME == "人口推計"
        assert stats_reader.TITLE == "人口推計（月報）"
        mock_get_response.assert_called_once()
    
    @patch('jpy_datareader.base._BaseReader._get_response')
    def test_read_dataframe_success(self, mock_get_response, stats_reader, expected_data_json):
        """Test successful DataFrame data retrieval."""
        # Setup mock response
        mock_response = Mock()
        mock_response.json.return_value = expected_data_json
        mock_get_response.return_value = mock_response
        
        # Execute
        result = stats_reader.read()
        
        # Verify
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 4
        assert 'value' in result.columns
        assert all(col in result.columns for col in ['tab_code', 'area_code', 'time_code'])
        
        # Check data types
        assert pd.api.types.is_numeric_dtype(result['value'])
        
        # Verify specific values
        assert result['value'].iloc[0] == 125000.0
        assert result['tab_code'].iloc[0] == '01'
        
        mock_get_response.assert_called_once()
    
    @patch('jpy_datareader.base._BaseReader._get_response')
    def test_parameter_construction(self, mock_get_response, mock_api_key, mock_stats_data_id):
        """Test correct parameter construction for API calls."""
        # Create reader with specific parameters
        reader = StatsDataReader(
            api_key=mock_api_key,
            statsDataId=mock_stats_data_id,
            cdArea="01000",
            cdTime="2024001000",
            limit=5000,
            metaGetFlg="Y"
        )
        
        # Setup mock
        mock_response = Mock()
        mock_response.json.return_value = {"GET_STATS_DATA": {"RESULT": {"STATUS": 0}}}
        mock_get_response.return_value = mock_response
        
        # Execute
        reader.read_json()
        
        # Verify parameters
        call_args = mock_get_response.call_args
        params = call_args[1]['params']
        
        assert params['appId'] == mock_api_key
        assert params['statsDataId'] == mock_stats_data_id
        assert params['cdArea'] == "01000"
        assert params['cdTime'] == "2024001000"
        assert params['limit'] == 5000
        assert params['metaGetFlg'] == "Y"

    # === 異常系テスト ===
    
    @patch('jpy_datareader.base._BaseReader._get_response')
    def test_api_error_status_non_zero(self, mock_get_response, stats_reader):
        """Test handling of API error status (STATUS != 0)."""
        error_response = {
            "GET_STATS_DATA": {
                "RESULT": {
                    "STATUS": 100,
                    "ERROR_MSG": "統計表が見つかりません。",
                    "DATE": "2025-07-13T10:30:00.000+09:00"
                }
            }
        }
        
        mock_response = Mock()
        mock_response.json.return_value = error_response
        mock_get_response.return_value = mock_response
        
        # Execute and verify error handling
        result = stats_reader.read_json()
        
        assert result == error_response
        assert stats_reader.STATUS == 100
        assert stats_reader.ERROR_MSG == "統計表が見つかりません。"
    
    @patch('jpy_datareader.base._BaseReader._get_response')
    def test_http_error_404(self, mock_get_response, stats_reader):
        """Test handling of HTTP 404 error."""
        mock_get_response.side_effect = RemoteDataError("Unable to read URL: https://api.e-stat.go.jp/rest/3.0/app/json/getStatsData")
        
        with pytest.raises(RemoteDataError, match="Unable to read URL"):
            stats_reader.read()
    
    @patch('jpy_datareader.base._BaseReader._get_response')
    def test_http_error_500(self, mock_get_response, stats_reader):
        """Test handling of HTTP 500 error."""
        mock_get_response.side_effect = RemoteDataError("Unable to read URL: server error 500")
        
        with pytest.raises(RemoteDataError, match="server error 500"):
            stats_reader.read_json()
    
    @patch('jpy_datareader.base._BaseReader._get_response')
    def test_missing_statistical_data_key(self, mock_get_response, stats_reader):
        """Test handling of missing STATISTICAL_DATA key."""
        incomplete_response = {
            "GET_STATS_DATA": {
                "RESULT": {
                    "STATUS": 0,
                    "ERROR_MSG": "",
                    "DATE": "2025-07-13T10:30:00.000+09:00"
                }
                # Missing STATISTICAL_DATA
            }
        }
        
        mock_response = Mock()
        mock_response.json.return_value = incomplete_response
        mock_get_response.return_value = mock_response
        
        # Should not raise exception, but return empty or minimal DataFrame
        result = stats_reader.read()
        assert isinstance(result, pd.DataFrame)
        # The result might be empty or have minimal data
    
    @patch('jpy_datareader.base._BaseReader._get_response')
    def test_missing_value_key(self, mock_get_response, stats_reader):
        """Test handling of missing VALUE key in DATA_INF."""
        incomplete_response = {
            "GET_STATS_DATA": {
                "RESULT": {"STATUS": 0},
                "STATISTICAL_DATA": {
                    "RESULT_INF": {"TOTAL_NUMBER": 0},
                    "TABLE_INF": {"STATISTICS_NAME": "テスト"},
                    "DATA_INF": {
                        # Missing VALUE key
                    }
                }
            }
        }
        
        mock_response = Mock()
        mock_response.json.return_value = incomplete_response
        mock_get_response.return_value = mock_response
        
        # Should handle gracefully
        result = stats_reader.read()
        assert isinstance(result, pd.DataFrame)
    
    @patch('jpy_datareader.base._BaseReader._get_response')
    def test_network_timeout_error(self, mock_get_response, stats_reader):
        """Test handling of network timeout."""
        mock_get_response.side_effect = RemoteDataError("Unable to read URL: timeout")
        
        with pytest.raises(RemoteDataError, match="timeout"):
            stats_reader.read()
    
    @patch('jpy_datareader.base._BaseReader._get_response')
    def test_connection_error(self, mock_get_response, stats_reader):
        """Test handling of connection error."""
        mock_get_response.side_effect = RemoteDataError("Unable to read URL: connection failed")
        
        with pytest.raises(RemoteDataError, match="connection failed"):
            stats_reader.read()

    # === 整合性テスト ===
    
    @patch('jpy_datareader.base._BaseReader._get_response')
    def test_data_consistency_with_expected_json(self, mock_get_response, stats_reader, expected_data_json):
        """Test data consistency with expected JSON structure."""
        mock_response = Mock()
        mock_response.json.return_value = expected_data_json
        mock_get_response.return_value = mock_response
        
        result = stats_reader.read_json()
        
        # Verify JSON structure consistency
        assert "GET_STATS_DATA" in result
        assert "STATISTICAL_DATA" in result["GET_STATS_DATA"]
        assert "DATA_INF" in result["GET_STATS_DATA"]["STATISTICAL_DATA"]
        assert "VALUE" in result["GET_STATS_DATA"]["STATISTICAL_DATA"]["DATA_INF"]
        
        # Verify data values
        values = result["GET_STATS_DATA"]["STATISTICAL_DATA"]["DATA_INF"]["VALUE"]
        assert len(values) == 4
        assert values[0]["$"] == "125000"
        assert values[0]["@tab"] == "01"
        assert values[0]["@area"] == "00000"
    
    @patch('jpy_datareader.base._BaseReader._get_response')
    def test_dataframe_consistency_with_expected_data(self, mock_get_response, stats_reader, expected_data_json):
        """Test DataFrame consistency with expected structure and values."""
        mock_response = Mock()
        mock_response.json.return_value = expected_data_json
        mock_get_response.return_value = mock_response
        
        result_df = stats_reader.read()
        
        # Verify DataFrame structure
        assert isinstance(result_df, pd.DataFrame)
        assert len(result_df) == 4
        
        # Verify required columns exist
        required_columns = ['tab_code', 'area_code', 'time_code', 'value']
        for col in required_columns:
            assert col in result_df.columns
        
        # Verify data types
        assert pd.api.types.is_numeric_dtype(result_df['value'])
        assert result_df['tab_code'].dtype == 'object'
        
        # Verify specific data points
        first_row = result_df.iloc[0]
        assert first_row['tab_code'] == '01'
        assert first_row['area_code'] == '00000'
        assert first_row['time_code'] == '2024001000'
        assert first_row['value'] == 125000.0
        
        # Verify all values are present and correct
        expected_values = [125000.0, 124950.0, 2500.0, 2498.0]
        actual_values = result_df['value'].tolist()
        assert actual_values == expected_values
    
    def test_csv_data_comparison(self, stats_reader):
        """Test comparison with CSV data file (if exists)."""
        csv_path = Path("tests/data/expected_data.csv")
        
        # Skip test if CSV file doesn't exist
        if not csv_path.exists():
            pytest.skip("Expected CSV data file not found")
        
        # Load expected data from CSV
        expected_df = pd.read_csv(csv_path)
        
        # Mock API response to return test data
        with patch('jpy_datareader.base._BaseReader._get_response') as mock_get_response:
            # Create mock response based on CSV data structure
            # This would need to be adapted based on actual CSV structure
            mock_response = Mock()
            mock_response.json.return_value = self._csv_to_api_response(expected_df)
            mock_get_response.return_value = mock_response
            
            result_df = stats_reader.read()
            
            # Compare DataFrames (allowing for column order differences)
            pd.testing.assert_frame_equal(
                result_df.sort_index(axis=1),
                expected_df.sort_index(axis=1),
                check_dtype=False  # Allow for minor type differences
            )
    
    def test_json_data_comparison(self, stats_reader):
        """Test comparison with JSON data file (if exists)."""
        json_path = Path("tests/data/expected_data.json")
        
        # Skip test if JSON file doesn't exist
        if not json_path.exists():
            pytest.skip("Expected JSON data file not found")
        
        # Load expected data from JSON
        with open(json_path, 'r', encoding='utf-8') as f:
            expected_json = json.load(f)
        
        with patch('jpy_datareader.base._BaseReader._get_response') as mock_get_response:
            mock_response = Mock()
            mock_response.json.return_value = expected_json
            mock_get_response.return_value = mock_response
            
            result_json = stats_reader.read_json()
            
            # Deep comparison of JSON structures
            assert result_json == expected_json
    
    # === パラメータテスト ===
    
    def test_invalid_api_key(self):
        """Test initialization with invalid API key."""
        with pytest.raises(ValueError, match="The e-Stat Application ID must be provided"):
            StatsDataReader(api_key=None, statsDataId="123456")
        
        with pytest.raises(ValueError, match="The e-Stat Application ID must be provided"):
            StatsDataReader(api_key="", statsDataId="123456")
    
    def test_url_construction(self, stats_reader):
        """Test URL construction for different endpoints."""
        assert "getStatsData" in stats_reader.url
        assert "api.e-stat.go.jp" in stats_reader.url
        assert "3.0" in stats_reader.url
    
    @patch('jpy_datareader.base._BaseReader._get_response')
    def test_missing_values_handling(self, mock_get_response, stats_reader):
        """Test handling of missing values with NOTE data."""
        response_with_notes = {
            "GET_STATS_DATA": {
                "RESULT": {"STATUS": 0},
                "STATISTICAL_DATA": {
                    "RESULT_INF": {"TOTAL_NUMBER": 2},
                    "TABLE_INF": {"STATISTICS_NAME": "テスト"},
                    "CLASS_INF": {
                        "CLASS_OBJ": [{
                            "@id": "tab",
                            "@name": "表章項目",
                            "CLASS": [{"@code": "01", "@name": "項目1", "@level": "1"}]
                        }]
                    },
                    "DATA_INF": {
                        "NOTE": [{"@char": "-", "$": "データなし"}],
                        "VALUE": [
                            {"@tab": "01", "$": "1000"},
                            {"@tab": "01", "$": "-"}
                        ]
                    }
                }
            }
        }
        
        mock_response = Mock()
        mock_response.json.return_value = response_with_notes
        mock_get_response.return_value = mock_response
        
        result_df = stats_reader.read()
        
        # Verify missing value handling
        assert pd.isna(result_df['value'].iloc[1])  # "-" should be converted to NaN
        assert result_df['value'].iloc[0] == 1000.0
    
    # === ヘルパーメソッド ===
    
    def _csv_to_api_response(self, csv_df: pd.DataFrame) -> dict:
        """Convert CSV DataFrame to mock API response format."""
        # This is a simplified conversion - would need to be adapted
        # based on actual CSV structure
        values = []
        for _, row in csv_df.iterrows():
            value_dict = {}
            for col in csv_df.columns:
                if col == 'value':
                    value_dict['$'] = str(row[col])
                else:
                    value_dict[f'@{col}'] = str(row[col])
            values.append(value_dict)
        
        return {
            "GET_STATS_DATA": {
                "RESULT": {"STATUS": 0},
                "STATISTICAL_DATA": {
                    "RESULT_INF": {"TOTAL_NUMBER": len(values)},
                    "TABLE_INF": {"STATISTICS_NAME": "テストデータ"},
                    "DATA_INF": {"VALUE": values}
                }
            }
        }


class TestEStatReader:
    """Test suite for base _eStatReader class."""
    
    def test_api_key_from_environment(self):
        """Test API key retrieval from environment variables."""
        with patch.dict('os.environ', {'E_STAT_API_KEY': 'env_test_key'}):
            reader = _eStatReader()
            assert reader.api_key == 'env_test_key'
    
    def test_url_generation(self):
        """Test URL generation for different endpoints."""
        reader = _eStatReader(api_key="test_key")
        
        assert reader.get_url("getStatsList").endswith("getStatsList?")
        assert reader.get_url("getStatsData").endswith("getStatsData?")
        assert reader.get_url("getMetaInfo").endswith("getMetaInfo?")
    
    def test_invalid_path_handling(self):
        """Test handling of invalid endpoint paths."""
        reader = _eStatReader(api_key="test_key")
        
        # Invalid path should default to getStatsData
        url = reader.get_url("invalidPath")
        assert "getStatsData" in url


# === テストデータファイル作成用のヘルパー関数 ===

def create_test_data_files():
    """Create test data files for consistency testing."""
    test_data_dir = Path("tests/data")
    test_data_dir.mkdir(parents=True, exist_ok=True)
    
    # Create expected JSON data
    expected_json = {
        "GET_STATS_DATA": {
            "RESULT": {"STATUS": 0, "ERROR_MSG": "", "DATE": "2025-07-13T10:30:00.000+09:00"},
            "STATISTICAL_DATA": {
                "RESULT_INF": {"TOTAL_NUMBER": 4},
                "TABLE_INF": {"STATISTICS_NAME": "人口推計"},
                "DATA_INF": {
                    "VALUE": [
                        {"@tab": "01", "@area": "00000", "@time": "2024001000", "$": "125000"},
                        {"@tab": "01", "@area": "00000", "@time": "2024002000", "$": "124950"},
                        {"@tab": "02", "@area": "01000", "@time": "2024001000", "$": "2500"},
                        {"@tab": "02", "@area": "01000", "@time": "2024002000", "$": "2498"}
                    ]
                }
            }
        }
    }
    
    with open(test_data_dir / "expected_data.json", 'w', encoding='utf-8') as f:
        json.dump(expected_json, f, ensure_ascii=False, indent=2)
    
    # Create expected CSV data
    expected_csv = pd.DataFrame({
        'tab_code': ['01', '01', '02', '02'],
        'area_code': ['00000', '00000', '01000', '01000'],
        'time_code': ['2024001000', '2024002000', '2024001000', '2024002000'],
        'value': [125000, 124950, 2500, 2498]
    })
    
    expected_csv.to_csv(test_data_dir / "expected_data.csv", index=False)



