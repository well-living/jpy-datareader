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
    def expected_single_item_response(self):
        """単一アイテムのレスポンスデータ（limit=1の場合）"""
        return {
            "GET_STATS_LIST": {
                "RESULT": {
                    "STATUS": 0,
                    "ERROR_MSG": "正常に終了しました。",
                    "DATE": "2025-07-14T17:53:19.905+09:00"
                },
                "PARAMETER": {
                    "LANG": "J",
                    "DATA_FORMAT": "J",
                    "LIMIT": 1
                },
                "DATALIST_INF": {
                    "NUMBER": 230034,
                    "RESULT_INF": {
                        "FROM_NUMBER": 1,
                        "TO_NUMBER": 1,
                        "NEXT_KEY": 2
                    },
                    "TABLE_INF": {
                        "@id": "0003288322",
                        "STAT_NAME": {"@code": "00020111", "$": "民間企業の勤務条件制度等調査"},
                        "GOV_ORG": {"@code": "00020", "$": "人事院"},
                        "STATISTICS_NAME": "民間企業の勤務条件制度等調査（民間企業退職給付調査） 統計表 １　定年制と定年退職者の継続雇用の状況",
                        "TITLE": {"@no": "1", "$": "（推計値） 定年制の状況"},
                        "CYCLE": "年次",
                        "SURVEY_DATE": "201601-201612",
                        "OPEN_DATE": "2019-03-20",
                        "SMALL_AREA": 0,
                        "COLLECT_AREA": "該当なし",
                        "MAIN_CATEGORY": {"@code": "03", "$": "労働・賃金"},
                        "SUB_CATEGORY": {"@code": "02", "$": "賃金・労働条件"},
                        "OVERALL_TOTAL_NUMBER": 25,
                        "UPDATED_DATE": "2019-03-30",
                        "STATISTICS_NAME_SPEC": {
                            "TABULATION_CATEGORY": "民間企業の勤務条件制度等調査（民間企業退職給付調査）",
                            "TABULATION_SUB_CATEGORY1": "統計表",
                            "TABULATION_SUB_CATEGORY2": "１　定年制と定年退職者の継続雇用の状況"
                        },
                        "DESCRIPTION": "",
                        "TITLE_SPEC": {
                            "TABLE_CATEGORY": "（推計値）",
                            "TABLE_NAME": "定年制の状況",
                            "TABLE_EXPLANATION": "１　事務・技術関係職種の従業員がいる企業41,314社について集計した。２　「定年年齢」内の数値は定年制がある企業を100とした場合の割合を示す。"
                        }
                    }
                }
            }
        }

    @pytest.fixture
    def expected_multiple_items_response(self):
        """複数アイテムのレスポンスデータ（実際のAPIから）"""
        return {
            "GET_STATS_LIST": {
                "RESULT": {
                    "STATUS": 0,
                    "ERROR_MSG": "正常に終了しました。",
                    "DATE": "2025-07-14T18:10:22.529+09:00"
                },
                "PARAMETER": {
                    "LANG": "J",
                    "SURVEY_YEARS": 2020,
                    "OPEN_YEARS": 202312,
                    "STATS_CODE": "00200521",
                    "EXPLANATION_GET_FLG": "N",
                    "DATA_FORMAT": "J"
                },
                "DATALIST_INF": {
                    "NUMBER": 12,
                    "RESULT_INF": {
                        "FROM_NUMBER": 1,
                        "TO_NUMBER": 12
                    },
                    "TABLE_INF": [
                        {
                            "@id": "0003412191",
                            "STAT_NAME": {"@code": "00200521", "$": "国勢調査"},
                            "GOV_ORG": {"@code": "00200", "$": "総務省"},
                            "STATISTICS_NAME": "時系列データ 世帯",
                            "TITLE": {
                                "@no": "7",
                                "$": "夫婦の就業・非就業（4区分）別一般世帯数（夫婦のいる一般世帯）－全国，都道府県（昭和55年～令和2年）"
                            },
                            "CYCLE": "-",
                            "SURVEY_DATE": "202001-202012",
                            "OPEN_DATE": "2023-12-01",
                            "SMALL_AREA": 0,
                            "COLLECT_AREA": "該当なし",
                            "MAIN_CATEGORY": {"@code": "02", "$": "人口・世帯"},
                            "SUB_CATEGORY": {"@code": "01", "$": "人口"},
                            "OVERALL_TOTAL_NUMBER": 2592,
                            "UPDATED_DATE": "2024-09-30",
                            "STATISTICS_NAME_SPEC": {
                                "TABULATION_CATEGORY": "時系列データ",
                                "TABULATION_SUB_CATEGORY1": "世帯"
                            },
                            "TITLE_SPEC": {
                                "TABLE_NAME": "夫婦の就業・非就業（4区分）別一般世帯数（夫婦のいる一般世帯）－全国，都道府県（昭和55年～令和2年）"
                            }
                        },
                        {
                            "@id": "0004003060",
                            "STAT_NAME": {"@code": "00200521", "$": "国勢調査"},
                            "GOV_ORG": {"@code": "00200", "$": "総務省"},
                            "STATISTICS_NAME": "時系列データ 従業地・通学地",
                            "TITLE": {
                                "@no": "1-2020",
                                "$": "常住地又は従業地・通学地別人口（夜間人口・昼間人口）－全国，都道府県，市区町村（令和2年）"
                            },
                            "CYCLE": "-",
                            "SURVEY_DATE": "202001-202012",
                            "OPEN_DATE": "2023-12-01",
                            "SMALL_AREA": 0,
                            "COLLECT_AREA": "該当なし",
                            "MAIN_CATEGORY": {"@code": "02", "$": "人口・世帯"},
                            "SUB_CATEGORY": {"@code": "01", "$": "人口"},
                            "OVERALL_TOTAL_NUMBER": 27510,
                            "UPDATED_DATE": "2024-09-30",
                            "STATISTICS_NAME_SPEC": {
                                "TABULATION_CATEGORY": "時系列データ",
                                "TABULATION_SUB_CATEGORY1": "従業地・通学地"
                            },
                            "TITLE_SPEC": {
                                "TABLE_NAME": "常住地又は従業地・通学地別人口（夜間人口・昼間人口）－全国，都道府県，市区町村（令和2年）"
                            }
                        }
                    ]
                }
            }
        }

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
            collectArea=1,
            statsCode="00200521",
            surveyYears=2020,
            openYears=202312,
            limit=10
        )
        assert reader.api_key == mock_api_key
        assert reader.lang == "E"
        assert reader.explanationGetFlg == "Y"
        assert reader.searchWord == "人口"
        assert reader.searchKind == 1
        assert reader.collectArea == 1
        assert reader.statsCode == "00200521"
        assert reader.surveyYears == 2020
        assert reader.openYears == 202312
        assert reader.limit == 10

    def test_init_without_api_key_raises_error(self):
        """APIキーなしの初期化で例外が発生することをテスト"""
        with pytest.raises(TypeError, match="missing 1 required positional argument"):
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
            explanationGetFlg="Y",
            statsCode="00200521",
            surveyYears=2020,
            openYears=202312,
            limit=5
        )
        params = reader.params
        assert params["appId"] == mock_api_key
        assert params["searchWord"] == "人口"
        assert params["searchKind"] == 1
        assert params["collectArea"] == 2
        assert params["explanationGetFlg"] == "Y"
        assert params["statsCode"] == "00200521"
        assert params["surveyYears"] == 2020
        assert params["openYears"] == 202312
        assert params["limit"] == 5

    @patch('jpy_datareader.estat._BaseReader._get_response')
    def test_read_single_item_success(self, mock_get_response, mock_api_key, expected_single_item_response):
        """正常系: 単一アイテム取得成功のテスト（limit=1）"""
        # モックレスポンスの設定
        mock_response = Mock()
        mock_response.json.return_value = expected_single_item_response
        mock_get_response.return_value = mock_response

        reader = StatsListReader(api_key=mock_api_key, limit=1)
        result = reader.read()

        # レスポンス取得が呼ばれたことを確認
        mock_get_response.assert_called_once()
        
        # 結果がDataFrameであることを確認
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        assert "id" in result.columns
        assert result.iloc[0]["id"] == "0003288322"
        assert result.iloc[0]["STAT_NAME"] == "民間企業の勤務条件制度等調査"
        
        # TITLEがオブジェクト形式の場合の処理確認
        assert "TITLE_no" in result.columns
        assert "TITLE" in result.columns
        assert result.iloc[0]["TITLE_no"] == "1"
        assert result.iloc[0]["TITLE"] == "（推計値） 定年制の状況"

        # インスタンス変数に結果が保存されていることを確認
        assert reader.STATUS == 0
        assert reader.NUMBER == 230034

    @patch('jpy_datareader.estat._BaseReader._get_response')
    def test_read_multiple_items_success(self, mock_get_response, mock_api_key, expected_multiple_items_response):
        """正常系: 複数アイテム取得成功のテスト"""
        # モックレスポンスの設定
        mock_response = Mock()
        mock_response.json.return_value = expected_multiple_items_response
        mock_get_response.return_value = mock_response

        reader = StatsListReader(
            api_key=mock_api_key,
            statsCode="00200521",
            surveyYears=2020,
            openYears=202312,
            explanationGetFlg="N"
        )
        result = reader.read()

        # レスポンス取得が呼ばれたことを確認
        mock_get_response.assert_called_once()
        
        # 結果がDataFrameであることを確認
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert "id" in result.columns
        assert result.iloc[0]["id"] == "0003412191"
        assert result.iloc[1]["id"] == "0004003060"
        
        # 国勢調査データの確認
        assert all(result["STAT_NAME"] == "国勢調査")
        assert all(result["GOV_ORG"] == "総務省")
        
        # TITLE構造の確認
        assert "TITLE_no" in result.columns
        assert "TITLE" in result.columns
        assert result.iloc[0]["TITLE_no"] == "7"
        assert result.iloc[1]["TITLE_no"] == "1-2020"

        # インスタンス変数に結果が保存されていることを確認
        assert reader.STATUS == 0
        assert reader.NUMBER == 12

    @patch('jpy_datareader.estat._BaseReader._get_response')
    def test_read_json_success(self, mock_get_response, mock_api_key, expected_multiple_items_response):
        """正常系: JSON取得成功のテスト"""
        # モックレスポンスの設定
        mock_response = Mock()
        mock_response.json.return_value = expected_multiple_items_response
        mock_get_response.return_value = mock_response

        reader = StatsListReader(api_key=mock_api_key)
        result = reader.read_json()

        # レスポンス取得が呼ばれたことを確認
        mock_get_response.assert_called_once()
        
        # 結果が期待されるJSONと一致することを確認
        assert result == expected_multiple_items_response
        
        # インスタンス変数に結果が保存されていることを確認
        assert reader.STATUS == 0
        assert reader.NUMBER == 12

    @patch('jpy_datareader.estat._BaseReader._get_response')
    def test_read_api_error_status(self, mock_get_response, mock_api_key):
        """異常系: API側でエラーステータスが返されるテスト"""
        error_response = {
            "GET_STATS_LIST": {
                "RESULT": {
                    "STATUS": 100,
                    "ERROR_MSG": "統計表IDが存在しません。",
                    "DATE": "2025-07-14T10:30:00.000+09:00"
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
        assert reader.STATUS == 100
        assert reader.ERROR_MSG == "統計表IDが存在しません。"

    @patch('jpy_datareader.estat._BaseReader._get_response')
    def test_read_communication_error(self, mock_get_response, mock_api_key):
        """異常系: 通信例外のテスト"""
        mock_get_response.side_effect = RemoteDataError("Unable to read URL")

        reader = StatsListReader(api_key=mock_api_key)
        
        with pytest.raises(RemoteDataError, match="Unable to read URL"):
            reader.read()

    @patch('jpy_datareader.estat._BaseReader._get_response')
    def test_read_empty_table_inf(self, mock_get_response, mock_api_key):
        """異常系: TABLE_INFが空の場合のテスト"""
        empty_response = {
            "GET_STATS_LIST": {
                "RESULT": {
                    "STATUS": 0,
                    "ERROR_MSG": "正常に終了しました。",
                    "DATE": "2025-07-14T10:30:00.000+09:00"
                },
                "PARAMETER": {
                    "LANG": "J",
                    "DATA_FORMAT": "J"
                },
                "DATALIST_INF": {
                    "NUMBER": 0,
                    "RESULT_INF": {
                        "FROM_NUMBER": 0,
                        "TO_NUMBER": 0
                    },
                    "TABLE_INF": []
                }
            }
        }
        
        mock_response = Mock()
        mock_response.json.return_value = empty_response
        mock_get_response.return_value = mock_response

        reader = StatsListReader(api_key=mock_api_key)
        result = reader.read()

        # 空の結果でもDataFrameが返されることを確認
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0
        assert reader.STATUS == 0
        assert reader.NUMBER == 0

    def test_data_consistency_with_actual_structure(self, mock_api_key, tmp_path):
        """整合性テスト: 実際のデータ構造との比較"""
        # 実際のAPIレスポンス構造に基づいたテストデータ
        test_data = {
            "GET_STATS_LIST": {
                "RESULT": {
                    "STATUS": 0,
                    "ERROR_MSG": "正常に終了しました。",
                    "DATE": "2025-07-14T17:53:19.905+09:00"
                },
                "PARAMETER": {
                    "LANG": "J",
                    "DATA_FORMAT": "J",
                    "LIMIT": 1
                },
                "DATALIST_INF": {
                    "NUMBER": 1,
                    "RESULT_INF": {
                        "FROM_NUMBER": 1,
                        "TO_NUMBER": 1
                    },
                    "TABLE_INF": {
                        "@id": "test001",
                        "STAT_NAME": {"@code": "00001", "$": "テスト統計"},
                        "GOV_ORG": {"@code": "00100", "$": "テスト省"},
                        "STATISTICS_NAME": "テスト調査",
                        "TITLE": {"@no": "1", "$": "テスト集計"},
                        "CYCLE": "年次",
                        "SURVEY_DATE": "202001-202012",
                        "OPEN_DATE": "2023-12-01",
                        "SMALL_AREA": 0,
                        "COLLECT_AREA": "全国",
                        "MAIN_CATEGORY": {"@code": "02", "$": "人口・世帯"},
                        "SUB_CATEGORY": {"@code": "01", "$": "人口"},
                        "OVERALL_TOTAL_NUMBER": 100,
                        "UPDATED_DATE": "2024-01-01",
                        "STATISTICS_NAME_SPEC": {
                            "TABULATION_CATEGORY": "テスト分類",
                            "TABULATION_SUB_CATEGORY1": "サブ分類1"
                        },
                        "TITLE_SPEC": {
                            "TABLE_NAME": "テスト表名"
                        }
                    }
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

            reader = StatsListReader(api_key=mock_api_key, limit=1)
            result_json = reader.read_json()
            result_df = reader.read()

            # レスポンスデータとファイルデータが一致することを確認
            assert result_json == expected_data
            
            # DataFrameの構造確認
            assert isinstance(result_df, pd.DataFrame)
            assert len(result_df) == 1
            assert result_df.iloc[0]["id"] == "test001"
            assert result_df.iloc[0]["STAT_NAME"] == "テスト統計"
            assert result_df.iloc[0]["TITLE_no"] == "1"
            assert result_df.iloc[0]["TITLE"] == "テスト集計"
            
            # 重要なキーが存在することを確認
            table_inf = result_json["GET_STATS_LIST"]["DATALIST_INF"]["TABLE_INF"]
            assert table_inf["@id"] == "test001"
            assert table_inf["STAT_NAME"]["$"] == "テスト統計"
            assert table_inf["TITLE"]["@no"] == "1"
            assert table_inf["TITLE"]["$"] == "テスト集計"


    @patch('jpy_datareader.estat._BaseReader._get_response')
    def test_complete_csv_data_validation(self, mock_get_response, mock_api_key, expected_multiple_items_response):
        """完全なCSVデータの検証テスト: 全12行のデータを確認"""
        # 実際のCSV出力に基づく期待データ
        expected_multiple_items_response["GET_STATS_LIST"]["DATALIST_INF"]["TABLE_INF"] = [
            {
                "@id": "0003412191",
                "STAT_NAME": {"@code": "00200521", "$": "国勢調査"},
                "GOV_ORG": {"@code": "00200", "$": "総務省"},
                "TITLE": {"@no": "7", "$": "夫婦の就業・非就業（4区分）別一般世帯数（夫婦のいる一般世帯）－全国，都道府県（昭和55年～令和2年）"},
                "MAIN_CATEGORY": {"@code": "02", "$": "人口・世帯"},
                "SUB_CATEGORY": {"@code": "01", "$": "人口"}
            },
            {
                "@id": "0004003060",
                "STAT_NAME": {"@code": "00200521", "$": "国勢調査"},
                "GOV_ORG": {"@code": "00200", "$": "総務省"},
                "TITLE": {"@no": "1-2020", "$": "常住地又は従業地・通学地別人口（夜間人口・昼間人口）－全国，都道府県，市区町村（令和2年）"},
                "MAIN_CATEGORY": {"@code": "02", "$": "人口・世帯"},
                "SUB_CATEGORY": {"@code": "01", "$": "人口"}
            },
            {
                "@id": "0004003061",
                "STAT_NAME": {"@code": "00200521", "$": "国勢調査"},
                "GOV_ORG": {"@code": "00200", "$": "総務省"},
                "TITLE": {"@no": "1-2020I", "$": "【不詳補完値】常住地又は従業地・通学地別人口（夜間人口・昼間人口）－全国，都道府県，市区町村（令和2年）"},
                "MAIN_CATEGORY": {"@code": "02", "$": "人口・世帯"},
                "SUB_CATEGORY": {"@code": "01", "$": "人口"}
            },
            {
                "@id": "0003412175",
                "STAT_NAME": {"@code": "00200521", "$": "国勢調査"},
                "GOV_ORG": {"@code": "00200", "$": "総務省"},
                "TITLE": {"@no": "1", "$": "労働力状態（3区分），男女別人口及び労働力率（15歳以上） － 全国（昭和25年～令和2年）"},
                "MAIN_CATEGORY": {"@code": "02", "$": "人口・世帯"},
                "SUB_CATEGORY": {"@code": "01", "$": "人口"}
            },
            {
                "@id": "0003412176",
                "STAT_NAME": {"@code": "00200521", "$": "国勢調査"},
                "GOV_ORG": {"@code": "00200", "$": "総務省"},
                "TITLE": {"@no": "2", "$": "労働力状態（3区分），男女別人口及び労働力率（15歳以上） － 都道府県（昭和25年～令和2年）"},
                "MAIN_CATEGORY": {"@code": "02", "$": "人口・世帯"},
                "SUB_CATEGORY": {"@code": "01", "$": "人口"}
            },
            {
                "@id": "0003410394",
                "STAT_NAME": {"@code": "00200521", "$": "国勢調査"},
                "GOV_ORG": {"@code": "00200", "$": "総務省"},
                "TITLE": {"@no": "3", "$": "労働力状態（8区分），年齢（5歳階級），男女別人口及び人口構成比［労働力状態別］（15歳以上） － 全国（昭和55年～令和2年）"},
                "MAIN_CATEGORY": {"@code": "02", "$": "人口・世帯"},
                "SUB_CATEGORY": {"@code": "01", "$": "人口"}
            },
            {
                "@id": "0003410395",
                "STAT_NAME": {"@code": "00200521", "$": "国勢調査"},
                "GOV_ORG": {"@code": "00200", "$": "総務省"},
                "TITLE": {"@no": "4", "$": "産業（大分類），男女別就業者数及び人口構成比［産業別］（15歳以上就業者）－全国（平成7年～令和2年）※平成19年11月改訂後"},
                "MAIN_CATEGORY": {"@code": "02", "$": "人口・世帯"},
                "SUB_CATEGORY": {"@code": "01", "$": "人口"}
            },
            {
                "@id": "0003410398",
                "STAT_NAME": {"@code": "00200521", "$": "国勢調査"},
                "GOV_ORG": {"@code": "00200", "$": "総務省"},
                "TITLE": {"@no": "5-2020", "$": "産業（大分類），男女別就業者数及び人口構成比［産業別］（15歳以上就業者） － 都道府県（平成17年～令和2年）"},
                "MAIN_CATEGORY": {"@code": "02", "$": "人口・世帯"},
                "SUB_CATEGORY": {"@code": "01", "$": "人口"}
            },
            {
                "@id": "0004003080",
                "STAT_NAME": {"@code": "00200521", "$": "国勢調査"},
                "GOV_ORG": {"@code": "00200", "$": "総務省"},
                "TITLE": {"@no": "6-2020", "$": "産業（小分類），従業上の地位（7区分），男女別就業者数（15歳以上）－全国（令和2年）"},
                "MAIN_CATEGORY": {"@code": "02", "$": "人口・世帯"},
                "SUB_CATEGORY": {"@code": "01", "$": "人口"}
            },
            {
                "@id": "0003410408",
                "STAT_NAME": {"@code": "00200521", "$": "国勢調査"},
                "GOV_ORG": {"@code": "00200", "$": "総務省"},
                "TITLE": {"@no": "7", "$": "職業（大分類），男女別就業者数及び人口構成比［職業別］（15歳以上就業者）－全国（平成7年～令和2年）"},
                "MAIN_CATEGORY": {"@code": "02", "$": "人口・世帯"},
                "SUB_CATEGORY": {"@code": "01", "$": "人口"}
            },
            {
                "@id": "0003410411",
                "STAT_NAME": {"@code": "00200521", "$": "国勢調査"},
                "GOV_ORG": {"@code": "00200", "$": "総務省"},
                "TITLE": {"@no": "8-2020", "$": "職業（大分類），男女別就業者数及び人口構成比［職業別］（15歳以上就業者）－都道府県（平成17年～令和2年）"},
                "MAIN_CATEGORY": {"@code": "02", "$": "人口・世帯"},
                "SUB_CATEGORY": {"@code": "01", "$": "人口"}
            },
            {
                "@id": "0004003081",
                "STAT_NAME": {"@code": "00200521", "$": "国勢調査"},
                "GOV_ORG": {"@code": "00200", "$": "総務省"},
                "TITLE": {"@no": "9-2020", "$": "職業（小分類），従業上の地位（7区分），男女別就業者数（15歳以上）－全国（令和2年）"},
                "MAIN_CATEGORY": {"@code": "02", "$": "人口・世帯"},
                "SUB_CATEGORY": {"@code": "01", "$": "人口"}
            }
        ]

        # モックレスポンスの設定
        mock_response = Mock()
        mock_response.json.return_value = expected_multiple_items_response
        mock_get_response.return_value = mock_response

        reader = StatsListReader(
            api_key=mock_api_key,
            statsCode="00200521",
            surveyYears=2020,
            openYears=202312,
            explanationGetFlg="N"
        )
        result_df = reader.read()

        # 実際のCSVデータとの完全一致検証
        expected_csv_data = [
            ("0003412191", "7", "夫婦の就業・非就業（4区分）別一般世帯数（夫婦のいる一般世帯）－全国，都道府県（昭和55年～令和2年）"),
            ("0004003060", "1-2020", "常住地又は従業地・通学地別人口（夜間人口・昼間人口）－全国，都道府県，市区町村（令和2年）"),
            ("0004003061", "1-2020I", "【不詳補完値】常住地又は従業地・通学地別人口（夜間人口・昼間人口）－全国，都道府県，市区町村（令和2年）"),
            ("0003412175", "1", "労働力状態（3区分），男女別人口及び労働力率（15歳以上） － 全国（昭和25年～令和2年）"),
            ("0003412176", "2", "労働力状態（3区分），男女別人口及び労働力率（15歳以上） － 都道府県（昭和25年～令和2年）"),
            ("0003410394", "3", "労働力状態（8区分），年齢（5歳階級），男女別人口及び人口構成比［労働力状態別］（15歳以上） － 全国（昭和55年～令和2年）"),
            ("0003410395", "4", "産業（大分類），男女別就業者数及び人口構成比［産業別］（15歳以上就業者）－全国（平成7年～令和2年）※平成19年11月改訂後"),
            ("0003410398", "5-2020", "産業（大分類），男女別就業者数及び人口構成比［産業別］（15歳以上就業者） － 都道府県（平成17年～令和2年）"),
            ("0004003080", "6-2020", "産業（小分類），従業上の地位（7区分），男女別就業者数（15歳以上）－全国（令和2年）"),
            ("0003410408", "7", "職業（大分類），男女別就業者数及び人口構成比［職業別］（15歳以上就業者）－全国（平成7年～令和2年）"),
            ("0003410411", "8-2020", "職業（大分類），男女別就業者数及び人口構成比［職業別］（15歳以上就業者）－都道府県（平成17年～令和2年）"),
            ("0004003081", "9-2020", "職業（小分類），従業上の地位（7区分），男女別就業者数（15歳以上）－全国（令和2年）")
        ]

        # データ数の確認
        assert len(result_df) == 12

        # 各行の詳細データ検証
        for i, (expected_id, expected_title_no, expected_title) in enumerate(expected_csv_data):
            assert result_df.iloc[i]["id"] == expected_id
            assert result_df.iloc[i]["TITLE_no"] == expected_title_no
            assert result_df.iloc[i]["TITLE"] == expected_title
            # 共通項目の確認
            assert result_df.iloc[i]["STAT_NAME_code"] == "00200521"
            assert result_df.iloc[i]["STAT_NAME"] == "国勢調査"
            assert result_df.iloc[i]["GOV_ORG_code"] == "00200"
            assert result_df.iloc[i]["GOV_ORG"] == "総務省"
            assert result_df.iloc[i]["MAIN_CATEGORY_code"] == "02"
            assert result_df.iloc[i]["MAIN_CATEGORY"] == "人口・世帯"
            assert result_df.iloc[i]["SUB_CATEGORY_code"] == "01"
            assert result_df.iloc[i]["SUB_CATEGORY"] == "人口"

        # CSV出力の形式確認
        csv_output = result_df.to_csv()
        csv_lines = csv_output.strip().split('\n')
        
        # ヘッダー確認
        expected_header_parts = [
            "id", "STAT_NAME_code", "STAT_NAME", "GOV_ORG_code", "GOV_ORG",
            "TITLE_no", "TITLE", "MAIN_CATEGORY_code", "MAIN_CATEGORY",
            "SUB_CATEGORY_code", "SUB_CATEGORY"
        ]
        header_line = csv_lines[0]
        for part in expected_header_parts:
            assert part in header_line

        # データ行数確認（ヘッダー + 12行）
        assert len(csv_lines) == 13



class TestMetaInfoReader:
    """MetaInfoReaderクラスの単体テスト"""

    @pytest.fixture
    def mock_api_key(self):
        """テスト用のAPIキー"""
        return "test_api_key_12345"

    @pytest.fixture
    def mock_stats_data_id(self):
        """テスト用の統計データID"""
        return "0002070010"

    @pytest.fixture
    def expected_metainfo_response(self):
        """実際のAPIレスポンス構造に基づくテストデータ"""
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
                        "STAT_NAME": "家計調査",
                        "GOV_ORG": "総務省",
                        "STATISTICS_NAME": "家計調査年報（家計収支編）",
                        "TITLE": "世帯属性別表"
                    },
                    "CLASS_INF": {
                        "CLASS_OBJ": [
                            {
                                "@id": "tab",
                                "@name": "表章項目",
                                "CLASS": [
                                    {
                                        "@code": "01",
                                        "@name": "金額",
                                        "@level": "",
                                        "@unit": "円"
                                    }
                                ]
                            },
                            {
                                "@id": "cat01",
                                "@name": "用途分類",
                                "CLASS": [
                                    {
                                        "@code": "012",
                                        "@name": "平均畳数（持家）",
                                        "@level": "1",
                                        "@unit": "畳"
                                    },
                                    {
                                        "@code": "013",
                                        "@name": "持家で住宅ローンを支払っている世帯の割合",
                                        "@level": "1",
                                        "@unit": "％"
                                    },
                                    {
                                        "@code": "014",
                                        "@name": "平均畳数（うち住宅ローンを支払っている世帯）",
                                        "@level": "1",
                                        "@unit": "畳"
                                    },
                                    {
                                        "@code": "015",
                                        "@name": "家賃・地代を支払っている世帯の割合",
                                        "@level": "1",
                                        "@unit": "％"
                                    },
                                    {
                                        "@code": "016",
                                        "@name": "平均畳数（家賃・地代を支払っている世帯）",
                                        "@level": "1",
                                        "@unit": "畳"
                                    },
                                    {
                                        "@code": "017",
                                        "@name": "農林漁家世帯の割合",
                                        "@level": "1",
                                        "@unit": "％"
                                    },
                                    {
                                        "@code": "018",
                                        "@name": "受取",
                                        "@level": "1",
                                        "@unit": "円"
                                    },
                                    {
                                        "@code": "019",
                                        "@name": "実収入",
                                        "@level": "2",
                                        "@unit": "円",
                                        "@parentCode": "018"
                                    },
                                    {
                                        "@code": "020",
                                        "@name": "経常収入",
                                        "@level": "3",
                                        "@unit": "円",
                                        "@parentCode": "019"
                                    },
                                    {
                                        "@code": "021",
                                        "@name": "勤め先収入",
                                        "@level": "4",
                                        "@unit": "円",
                                        "@parentCode": "020"
                                    },
                                    {
                                        "@code": "022",
                                        "@name": "世帯主収入",
                                        "@level": "5",
                                        "@unit": "円",
                                        "@parentCode": "021"
                                    },
                                    {
                                        "@code": "023",
                                        "@name": "世帯主収入（男）",
                                        "@level": "7",
                                        "@unit": "円",
                                        "@parentCode": "022"
                                    },
                                    {
                                        "@code": "024",
                                        "@name": "定期収入",
                                        "@level": "6",
                                        "@unit": "円",
                                        "@parentCode": "022"
                                    },
                                    {
                                        "@code": "025",
                                        "@name": "臨時収入",
                                        "@level": "7",
                                        "@unit": "円",
                                        "@parentCode": "267"
                                    },
                                    {
                                        "@code": "026",
                                        "@name": "賞与",
                                        "@level": "7",
                                        "@unit": "円",
                                        "@parentCode": "267"
                                    },
                                    {
                                        "@code": "027",
                                        "@name": "世帯主の配偶者の収入",
                                        "@level": "5",
                                        "@unit": "円",
                                        "@parentCode": "021"
                                    },
                                    {
                                        "@code": "028",
                                        "@name": "世帯主の配偶者の収入（女）",
                                        "@level": "7",
                                        "@unit": "円",
                                        "@parentCode": "027"
                                    },
                                    {
                                        "@code": "029",
                                        "@name": "他の世帯員収入",
                                        "@level": "5",
                                        "@unit": "円",
                                        "@parentCode": "021"
                                    },
                                    {
                                        "@code": "267",
                                        "@name": "臨時収入・賞与",
                                        "@level": "6",
                                        "@unit": "円",
                                        "@parentCode": "022"
                                    }
                                ]
                            },
                            {
                                "@id": "time",
                                "@name": "時間軸",
                                "CLASS": [
                                    {
                                        "@code": "2020000000",
                                        "@name": "2020年",
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

    def test_init_with_hierarchy_enabled(self, mock_api_key, mock_stats_data_id):
        """階層機能有効での初期化テスト"""
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
    def test_read_returns_largest_non_time_dataframe(self, mock_get_response, mock_api_key, mock_stats_data_id, expected_metainfo_response):
        """read()メソッドが最大行数のDataFrame（timeを除く）を返すことのテスト"""
        mock_response = Mock()
        mock_response.json.return_value = expected_metainfo_response
        mock_get_response.return_value = mock_response

        reader = MetaInfoReader(api_key=mock_api_key, statsDataId=mock_stats_data_id)
        result = reader.read()

        # レスポンス取得が呼ばれたことを確認
        mock_get_response.assert_called_once()
        
        # 結果がDataFrameであることを確認
        assert isinstance(result, pd.DataFrame)
        
        # 用途分類（cat01）が最も多い行数を持つため、それが返されることを確認
        # expected_metainfo_responseでは用途分類に19個のCLASSが含まれている
        assert len(result) == 19
        
        # 日本語列名が適用されていることを確認
        expected_columns = ["用途分類コード", "用途分類", "用途分類階層レベル", "用途分類単位", "用途分類親コード"]
        for col in expected_columns:
            assert col in result.columns

        # インスタンス変数に結果が保存されていることを確認
        assert reader.STATUS == 0
        assert reader.STAT_NAME == "家計調査"

    @patch('jpy_datareader.estat._BaseReader._get_response')
    def test_read_class_objs_structure(self, mock_get_response, mock_api_key, mock_stats_data_id, expected_metainfo_response):
        """read_class_objs()の結果構造テスト"""
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

        # 特定のクラスIDと名前を確認
        ids_and_names = [(class_data["id"], class_data["name"]) for class_data in result]
        assert ("tab", "表章項目") in ids_and_names
        assert ("cat01", "用途分類") in ids_and_names
        assert ("time", "時間軸") in ids_and_names

        # 表章項目の詳細確認
        tab_data = next(item for item in result if item["id"] == "tab")
        tab_df = tab_data["meta_dataframe"]
        assert len(tab_df) == 1
        assert "表章項目コード" in tab_df.columns
        assert tab_df.iloc[0]["表章項目コード"] == "01"
        assert tab_df.iloc[0]["表章項目"] == "金額"

        # 用途分類の詳細確認
        cat01_data = next(item for item in result if item["id"] == "cat01")
        cat01_df = cat01_data["meta_dataframe"]
        assert len(cat01_df) == 19
        assert "用途分類コード" in cat01_df.columns
        assert "用途分類親コード" in cat01_df.columns


    @patch('jpy_datareader.estat._BaseReader._get_response')
    def test_read_json_returns_raw_response(self, mock_get_response, mock_api_key, mock_stats_data_id, expected_metainfo_response):
        """read_json()が生のJSONレスポンスを返すことのテスト"""
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
    def test_level_column_handling_with_empty_strings(self, mock_get_response, mock_api_key, mock_stats_data_id):
        """@levelに空文字列が含まれる場合の処理テスト"""
        response_with_empty_level = {
            "GET_META_INFO": {
                "RESULT": {"STATUS": 0, "ERROR_MSG": "", "DATE": "2025-01-15T10:30:00.000+09:00"},
                "PARAMETER": {"LANG": "J", "DATA_FORMAT": "json"},
                "METADATA_INF": {
                    "TABLE_INF": {"STAT_NAME": "テスト統計"},
                    "CLASS_INF": {
                        "CLASS_OBJ": [{
                            "@id": "test_tab",
                            "@name": "表章項目",
                            "CLASS": [
                                {"@code": "01", "@name": "金額", "@level": "", "@unit": "円"},  # 空文字列
                                {"@code": "02", "@name": "人数", "@level": "1", "@unit": "人"}
                            ]
                        }]
                    }
                }
            }
        }
        
        mock_response = Mock()
        mock_response.json.return_value = response_with_empty_level
        mock_get_response.return_value = mock_response

        reader = MetaInfoReader(api_key=mock_api_key, statsDataId=mock_stats_data_id)
        result = reader.read_class_objs()

        # 表章項目のDataFrameを取得
        tab_data = next(item for item in result if item["id"] == "test_tab")
        tab_df = tab_data["meta_dataframe"]
        
        # 階層レベル列が適切に処理されていることを確認
        assert "表章項目階層レベル" in tab_df.columns
        
        # 空文字列がNAに変換されていることを確認
        level_column = tab_df["表章項目階層レベル"]
        assert pd.isna(level_column.iloc[0])  # 空文字列 -> NA
        assert level_column.iloc[1] == 1      # "1" -> 1

    @patch('jpy_datareader.estat._BaseReader._get_response')
    def test_error_status_handling(self, mock_get_response, mock_api_key, mock_stats_data_id):
        """API側エラーステータスの処理テスト"""
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

        # エラーステータスでも空のリストが返される
        assert isinstance(result, list)
        assert len(result) == 0
        assert reader.STATUS == 1
        assert reader.ERROR_MSG == "統計表IDが存在しません。"

    @patch('jpy_datareader.estat._BaseReader._get_response')
    def test_missing_class_inf_handling(self, mock_get_response, mock_api_key, mock_stats_data_id):
        """CLASS_INFが存在しない場合の処理テスト"""
        incomplete_response = {
            "GET_META_INFO": {
                "RESULT": {"STATUS": 0, "ERROR_MSG": "", "DATE": "2025-01-15T10:30:00.000+09:00"},
                "METADATA_INF": {
                    "TABLE_INF": {"STAT_NAME": "テスト統計"}
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
    def test_communication_error_handling(self, mock_get_response, mock_api_key, mock_stats_data_id):
        """通信例外の処理テスト"""
        mock_get_response.side_effect = RemoteDataError("Unable to read URL")

        reader = MetaInfoReader(api_key=mock_api_key, statsDataId=mock_stats_data_id)
        
        with pytest.raises(RemoteDataError, match="Unable to read URL"):
            reader.read()

    def test_create_class_dataframe_with_list_input(self, mock_api_key, mock_stats_data_id):
        """_create_class_dataframe()のリスト入力処理テスト"""
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
        
        # レベルがInt64型に変換されていることを確認
        assert result["@level"].dtype == "Int64"

    def test_create_class_dataframe_with_dict_input(self, mock_api_key, mock_stats_data_id):
        """_create_class_dataframe()の辞書入力処理テスト"""
        reader = MetaInfoReader(api_key=mock_api_key, statsDataId=mock_stats_data_id)
        
        class_data = {"@code": "001", "@name": "テスト", "@level": "1"}
        
        result = reader._create_class_dataframe(class_data)
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        assert result.iloc[0]["@code"] == "001"

    def test_column_name_transformation_with_prefix(self, mock_api_key, mock_stats_data_id):
        """列名変換のプレフィックス機能テスト"""
        # プレフィックスありの場合
        reader_with_prefix = MetaInfoReader(
            api_key=mock_api_key,
            statsDataId=mock_stats_data_id,
            prefix_colname_with_classname=True
        )
        
        class_data = [{"@code": "001", "@name": "テスト", "@level": "1", "@unit": "円"}]
        raw_df = pd.DataFrame(class_data)
        result_with_prefix = reader_with_prefix._apply_colname_transformations(raw_df, "用途分類")
        
        # プレフィックスありの場合は日本語列名にクラス名が含まれる
        expected_columns = ["用途分類コード", "用途分類", "用途分類階層レベル", "用途分類単位"]
        for col in expected_columns:
            assert col in result_with_prefix.columns
        
        # プレフィックスなしの場合
        reader_without_prefix = MetaInfoReader(
            api_key=mock_api_key,
            statsDataId=mock_stats_data_id,
            prefix_colname_with_classname=False
        )
        
        result_without_prefix = reader_without_prefix._apply_colname_transformations(raw_df, "用途分類")
        
        # プレフィックスなしの場合の列名確認
        expected_columns_no_prefix = ["コード", "用途分類", "階層レベル", "単位"]  # クラス名列のみ例外
        for col in expected_columns_no_prefix:
            assert col in result_without_prefix.columns

    def test_time_class_exclusion_in_read(self, mock_api_key, mock_stats_data_id, expected_metainfo_response):
        """read()メソッドがtimeクラスを除外することのテスト"""
        # timeクラスが最大行数を持つケースをテスト
        modified_response = expected_metainfo_response.copy()
        
        # timeクラスに多くのデータを追加
        time_class = {
            "@id": "time",
            "@name": "時間軸",
            "CLASS": [
                {"@code": f"202{i}000000", "@name": f"202{i}年", "@level": "1"}
                for i in range(0, 25)  # 25個のデータ（用途分類の19個より多い）
            ]
        }
        modified_response["GET_META_INFO"]["METADATA_INF"]["CLASS_INF"]["CLASS_OBJ"][2] = time_class
        
        with patch('jpy_datareader.estat._BaseReader._get_response') as mock_get_response:
            mock_response = Mock()
            mock_response.json.return_value = modified_response
            mock_get_response.return_value = mock_response

            reader = MetaInfoReader(api_key=mock_api_key, statsDataId=mock_stats_data_id)
            result = reader.read()

            # timeクラスが除外され、用途分類（19行）が返されることを確認
            assert len(result) == 19
            assert "用途分類コード" in result.columns


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



