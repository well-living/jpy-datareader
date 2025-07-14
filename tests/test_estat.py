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
from jpy_datareader.estat import _eStatReader, StatsListReader, MetaInfoReader, StatsDataReader, colname_to_japanese
from jpy_datareader._utils import RemoteDataError



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



class TestStatsDataReader:
    """Test suite for StatsDataReader class."""
    
    # === Fixtures ===
    
    @pytest.fixture
    def mock_api_key(self):
        """Fixture providing a mock API key."""
        return "test_api_key_12345"
    
    @pytest.fixture
    def mock_stats_data_id(self):
        """Fixture providing a mock statistics data ID."""
        return "0002070010"
    
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
        """Fixture providing expected JSON response data based on actual structure."""
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
                        "STATISTICS_NAME": "家計調査",
                        "TITLE": "用途分類 用途分類（世帯主の年齢階級別）",
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
                                    {"@code": "01", "@name": "支出金額", "@level": "1"},
                                    {"@code": "02", "@name": "購入数量", "@level": "1"}
                                ]
                            },
                            {
                                "@id": "cat01",
                                "@name": "用途分類",
                                "CLASS": [
                                    {"@code": "001", "@name": "食料", "@level": "1", "@unit": "円", "@parentCode": ""},
                                    {"@code": "002", "@name": "住居", "@level": "1", "@unit": "円", "@parentCode": ""}
                                ]
                            },
                            {
                                "@id": "cat02",
                                "@name": "世帯区分",
                                "CLASS": [
                                    {"@code": "01", "@name": "二人以上の世帯", "@level": "1"},
                                    {"@code": "02", "@name": "単身世帯", "@level": "2"}
                                ]
                            },
                            {
                                "@id": "cat03",
                                "@name": "世帯主の年齢階級",
                                "CLASS": [
                                    {"@code": "00", "@name": "全体", "@level": "1"},
                                    {"@code": "01", "@name": "30歳未満", "@level": "2"}
                                ]
                            },
                            {
                                "@id": "area",
                                "@name": "地域区分",
                                "CLASS": [
                                    {"@code": "00000", "@name": "全国", "@level": "1"},
                                    {"@code": "01000", "@name": "北海道", "@level": "2"}
                                ]
                            },
                            {
                                "@id": "time",
                                "@name": "時間軸（月次）",
                                "CLASS": [
                                    {"@code": "2024001000", "@name": "2024年1月", "@level": "1"},
                                    {"@code": "2024002000", "@name": "2024年2月", "@level": "1"}
                                ]
                            }
                        ]
                    },
                    "DATA_INF": {
                        "NOTE": [{"@char": "-", "$": "データなし"}],
                        "VALUE": [
                            {"@tab": "01", "@cat01": "001", "@cat02": "01", "@cat03": "00", "@area": "00000", "@time": "2024001000", "@unit": "一万分比", "$": "10000"},
                            {"@tab": "01", "@cat01": "001", "@cat02": "01", "@cat03": "00", "@area": "00000", "@time": "2024002000", "@unit": "一万分比", "$": "33"},
                            {"@tab": "02", "@cat01": "002", "@cat02": "02", "@cat03": "01", "@area": "01000", "@time": "2024001000", "@unit": "一万分比", "$": "29"},
                            {"@tab": "02", "@cat01": "002", "@cat02": "02", "@cat03": "01", "@area": "01000", "@time": "2024002000", "@unit": "一万分比", "$": "-"}
                        ]
                    }
                }
            }
        }
    
    @pytest.fixture
    def small_test_data(self, expected_data_json):
        """Fixture providing small dataset for lightweight tests."""
        small_data = expected_data_json.copy()
        small_data["GET_STATS_DATA"]["STATISTICAL_DATA"]["DATA_INF"]["VALUE"] = [
            {"@tab": "01", "@cat01": "001", "@cat02": "01", "@cat03": "00", "@area": "00000", "@time": "2024001000", "@unit": "一万分比", "$": "10000"},
            {"@tab": "01", "@cat01": "001", "@cat02": "01", "@cat03": "00", "@area": "00000", "@time": "2024002000", "@unit": "一万分比", "$": "-"}
        ]
        small_data["GET_STATS_DATA"]["STATISTICAL_DATA"]["RESULT_INF"]["TOTAL_NUMBER"] = 2
        small_data["GET_STATS_DATA"]["STATISTICAL_DATA"]["TABLE_INF"]["OVERALL_TOTAL_NUMBER"] = 2
        return small_data
    
    @pytest.fixture
    def multi_unit_data(self):
        """Fixture providing data with multiple units for split_by_unit tests."""
        return {
            "GET_STATS_DATA": {
                "RESULT": {"STATUS": 0},
                "STATISTICAL_DATA": {
                    "RESULT_INF": {"TOTAL_NUMBER": 4},
                    "TABLE_INF": {"STATISTICS_NAME": "テスト", "OVERALL_TOTAL_NUMBER": 4},
                    "CLASS_INF": {
                        "CLASS_OBJ": [{
                            "@id": "tab",
                            "@name": "表章項目",
                            "CLASS": [
                                {"@code": "01", "@name": "項目1", "@level": "1"},
                                {"@code": "02", "@name": "項目2", "@level": "1"}
                            ]
                        }]
                    },
                    "DATA_INF": {
                        "VALUE": [
                            {"@tab": "01", "@unit": "円", "$": "1000"},
                            {"@tab": "01", "@unit": "円", "$": "2000"},
                            {"@tab": "02", "@unit": "千人", "$": "100"},
                            {"@tab": "02", "@unit": "千人", "$": "200"}
                        ]
                    }
                }
            }
        }
    
    # === テスト用ヘルパーメソッド ===
    
    def _setup_mock_response(self, mock_get_response, response_data):
        """共通のモックレスポンス設定."""
        mock_response = Mock()
        mock_response.json.return_value = response_data
        mock_get_response.return_value = mock_response
        return mock_response
    
    def _verify_basic_dataframe_structure(self, df, expected_length=None, lang="J"):
        """DataFrame基本構造の検証."""
        assert isinstance(df, pd.DataFrame)
        if expected_length:
            assert len(df) == expected_length
        
        if lang == "E":
            assert 'value' in df.columns
            assert 'tab_code' in df.columns
            assert pd.api.types.is_numeric_dtype(df['value'])
        else:
            assert '値' in df.columns
            assert '表章項目コード' in df.columns
            assert pd.api.types.is_numeric_dtype(df['値'])
    
    # === 初期化・設定テスト ===
    
    @patch('jpy_datareader.base._BaseReader._get_response')
    def test_parameter_construction(self, mock_get_response, mock_api_key, mock_stats_data_id):
        """Test correct parameter construction for API calls."""
        reader = StatsDataReader(
            api_key=mock_api_key,
            statsDataId=mock_stats_data_id,
            cdArea="01000",
            cdTime="2024001000",
            limit=5000,
            metaGetFlg="Y"
        )
        
        self._setup_mock_response(mock_get_response, {"GET_STATS_DATA": {"RESULT": {"STATUS": 0}}})
        reader.read_json()
        
        call_args = mock_get_response.call_args
        params = call_args[1]['params']
        
        assert params['appId'] == mock_api_key
        assert params['statsDataId'] == mock_stats_data_id
        assert params['cdArea'] == "01000"
        assert params['cdTime'] == "2024001000"
        assert params['limit'] == 5000
        assert params['metaGetFlg'] == "Y"
    
    # === 正常系・基本機能テスト ===
    
    @patch('jpy_datareader.base._BaseReader._get_response')
    def test_read_json_success(self, mock_get_response, stats_reader, expected_data_json):
        """Test successful JSON data retrieval."""
        self._setup_mock_response(mock_get_response, expected_data_json)
        
        result = stats_reader.read_json()
        
        assert result == expected_data_json
        assert stats_reader.STATUS == 0
        assert stats_reader.STATISTICS_NAME == "家計調査"
        assert stats_reader.TITLE == "用途分類 用途分類（世帯主の年齢階級別）"
        mock_get_response.assert_called_once()
    
    @patch('jpy_datareader.base._BaseReader._get_response')
    def test_read_dataframe_japanese(self, mock_get_response, stats_reader, expected_data_json):
        """Test successful DataFrame data retrieval with Japanese column names."""
        self._setup_mock_response(mock_get_response, expected_data_json)
        
        result = stats_reader.read()
        
        self._verify_basic_dataframe_structure(result, expected_length=4, lang="J")
        
        # Verify specific values
        assert result['値'].iloc[0] == 10000.0
        assert result['表章項目コード'].iloc[0] == '01'
        assert result['単位'].iloc[0] == '一万分比'
        
        # Check missing value handling
        assert pd.isna(result['値'].iloc[3])  # "-" should be converted to NaN
    
    @patch('jpy_datareader.base._BaseReader._get_response')
    def test_read_dataframe_english(self, mock_get_response, mock_api_key, mock_stats_data_id, expected_data_json):
        """Test successful DataFrame data retrieval with English column names."""
        reader = StatsDataReader(
            api_key=mock_api_key,
            statsDataId=mock_stats_data_id,
            lang="E",
            retry_count=1,
            timeout=5
        )
        
        self._setup_mock_response(mock_get_response, expected_data_json)
        
        result = reader.read()
        
        self._verify_basic_dataframe_structure(result, expected_length=4, lang="E")
        
        # Verify specific values
        assert result['value'].iloc[0] == 10000.0
        assert result['tab_code'].iloc[0] == '01'
        assert result['unit'].iloc[0] == '一万分比'
        
        # Verify tab_colname setting
        assert reader.tab_colname == 'tab_name'
    
    @patch('jpy_datareader.base._BaseReader._get_response')
    def test_split_by_units_functionality(self, mock_get_response, stats_reader, multi_unit_data):
        """Test split_by_units functionality."""
        self._setup_mock_response(mock_get_response, multi_unit_data)
        
        # Test regular read
        regular_result = stats_reader.read()
        assert len(regular_result) == 4
        assert set(regular_result['単位'].unique()) == {'円', '千人'}
        
        # Reset mock for second call
        self._setup_mock_response(mock_get_response, multi_unit_data)
        
        # Test split by units
        split_result = stats_reader.read(split_by_unit=True)
        
        assert isinstance(split_result, dict)
        assert set(split_result.keys()) == {'円', '千人'}
        
        # Verify data integrity in splits
        yen_data = split_result['円']
        assert len(yen_data) == 2
        assert all(yen_data['単位'] == '円')
        assert yen_data['値'].tolist() == [1000.0, 2000.0]
        
        sen_nin_data = split_result['千人']
        assert len(sen_nin_data) == 2
        assert all(sen_nin_data['単位'] == '千人')
        assert sen_nin_data['値'].tolist() == [100.0, 200.0]
    
    # === データ整合性テスト ===
    
    @patch('jpy_datareader.base._BaseReader._get_response')
    def test_dataframe_consistency_lightweight(self, mock_get_response, stats_reader, small_test_data):
        """Test DataFrame consistency with lightweight dataset."""
        self._setup_mock_response(mock_get_response, small_test_data)
        
        result_df = stats_reader.read()
        
        self._verify_basic_dataframe_structure(result_df, expected_length=2, lang="J")
        
        # Verify required columns exist
        required_columns = ['表章項目コード', '用途分類コード', '世帯区分コード', '世帯主の年齢階級コード', 
                           '地域区分コード', '時間軸（月次）コード', '単位', '値']
        for col in required_columns:
            assert col in result_df.columns
        
        # Verify specific data points
        first_row = result_df.iloc[0]
        assert first_row['表章項目コード'] == '01'
        assert first_row['用途分類コード'] == '001'
        assert first_row['値'] == 10000.0
        
        # Verify missing value handling
        assert pd.isna(result_df['値'].iloc[1])
    
    @patch('jpy_datareader.base._BaseReader._get_response')
    def test_value_consistency_across_processing_steps(self, mock_get_response, stats_reader, expected_data_json):
        """Test that values remain consistent across all processing steps."""
        self._setup_mock_response(mock_get_response, expected_data_json)
        
        # Get JSON data
        json_data = stats_reader.read_json()
        
        # Step 1: Extract raw values
        raw_values = [v["$"] for v in json_data["GET_STATS_DATA"]["STATISTICAL_DATA"]["DATA_INF"]["VALUE"]]
        expected_raw = ["10000", "33", "29", "-"]
        assert raw_values == expected_raw
        
        # Step 2: After _statsjson_to_dataframe
        value_df = stats_reader._statsjson_to_dataframe(json_data)
        assert value_df['value'].tolist() == expected_raw
        
        # Step 3: After _handle_missing_values
        value_df_handled = stats_reader._handle_missing_values(value_df, json_data)
        expected_after_handling = [10000.0, 33.0, 29.0, float('nan')]
        actual_handled = value_df_handled['value'].tolist()
        assert actual_handled[:3] == expected_after_handling[:3]
        assert pd.isna(actual_handled[3])
        
        # Step 4: Final result should maintain value consistency
        result_df = stats_reader.read()
        actual_final = result_df['値'].tolist()
        assert actual_final[:3] == expected_after_handling[:3]
        assert pd.isna(actual_final[3])
    
    @patch('jpy_datareader.base._BaseReader._get_response')
    def test_language_setting_consistency(self, mock_get_response, mock_api_key, mock_stats_data_id, expected_data_json):
        """Test consistency between Japanese and English language settings."""
        jp_reader = StatsDataReader(api_key=mock_api_key, statsDataId=mock_stats_data_id, lang=None)
        en_reader = StatsDataReader(api_key=mock_api_key, statsDataId=mock_stats_data_id, lang="E")
        
        # Test Japanese
        self._setup_mock_response(mock_get_response, expected_data_json)
        jp_result = jp_reader.read()
        
        # Reset mock for English
        self._setup_mock_response(mock_get_response, expected_data_json)
        en_result = en_reader.read()
        
        # Verify data values are identical
        jp_values = jp_result['値'].tolist()
        en_values = en_result['value'].tolist()
        
        jp_non_nan = [v for v in jp_values if not pd.isna(v)]
        en_non_nan = [v for v in en_values if not pd.isna(v)]
        assert jp_non_nan == en_non_nan
        
        # Verify NaN positions match
        jp_nan_mask = jp_result['値'].isna()
        en_nan_mask = en_result['value'].isna()
        # 名前を統一してから比較
        jp_nan_mask.name = None
        en_nan_mask.name = None
        pd.testing.assert_series_equal(jp_nan_mask, en_nan_mask)
        # Verify tab_colname setting
        assert jp_reader.tab_colname == '表章項目'
        assert en_reader.tab_colname == 'tab_name'
    
    # === 欠損値処理テスト ===
    
    @patch('jpy_datareader.base._BaseReader._get_response')
    def test_missing_values_handling_multiple_notes(self, mock_get_response, stats_reader):
        """Test handling of missing values with multiple NOTE data."""
        response_with_multiple_notes = {
            "GET_STATS_DATA": {
                "RESULT": {"STATUS": 0},
                "STATISTICAL_DATA": {
                    "RESULT_INF": {"TOTAL_NUMBER": 3},
                    "TABLE_INF": {"STATISTICS_NAME": "テスト"},
                    "CLASS_INF": {
                        "CLASS_OBJ": [{
                            "@id": "tab",
                            "@name": "表章項目",
                            "CLASS": [{"@code": "01", "@name": "項目1", "@level": "1"}]
                        }]
                    },
                    "DATA_INF": {
                        "NOTE": [
                            {"@char": "-", "$": "データなし"},
                            {"@char": "***", "$": "秘匿"}
                        ],
                        "VALUE": [
                            {"@tab": "01", "@unit": "一万分比", "$": "1000"},
                            {"@tab": "01", "@unit": "一万分比", "$": "-"},
                            {"@tab": "01", "@unit": "一万分比", "$": "***"}
                        ]
                    }
                }
            }
        }
        
        self._setup_mock_response(mock_get_response, response_with_multiple_notes)
        result_df = stats_reader.read()
        
        assert result_df['値'].iloc[0] == 1000.0
        assert pd.isna(result_df['値'].iloc[1])  # "-" should be NaN
        assert pd.isna(result_df['値'].iloc[2])  # "***" should be NaN
    
    @patch('jpy_datareader.base._BaseReader._get_response')
    def test_no_missing_values_no_notes(self, mock_get_response, stats_reader):
        """Test handling when there are no missing values and no NOTE data."""
        response_without_notes = {
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
                        "VALUE": [
                            {"@tab": "01", "@unit": "一万分比", "$": "1000"},
                            {"@tab": "01", "@unit": "一万分比", "$": "2000"}
                        ]
                    }
                }
            }
        }
        
        self._setup_mock_response(mock_get_response, response_without_notes)
        result_df = stats_reader.read()
        
        assert not result_df['値'].isna().any()
        assert result_df['値'].iloc[0] == 1000.0
        assert result_df['値'].iloc[1] == 2000.0
    
    # === パラメータテスト ===
    
    @patch('jpy_datareader.base._BaseReader._get_response')
    def test_replace_sp_char_parameter(self, mock_get_response, mock_api_key, mock_stats_data_id):
        """Test replaceSpChar parameter functionality."""
        for replace_val in [0, 1, 2, 3]:
            reader = StatsDataReader(
                api_key=mock_api_key,
                statsDataId=mock_stats_data_id,
                replaceSpChar=replace_val
            )
            
            self._setup_mock_response(mock_get_response, {"GET_STATS_DATA": {"RESULT": {"STATUS": 0}}})
            reader.read_json()
            
            call_args = mock_get_response.call_args
            params = call_args[1]['params']
            assert params['replaceSpChar'] == replace_val
    
    @patch('jpy_datareader.base._BaseReader._get_response')
    def test_limit_parameter_handling(self, mock_get_response, mock_api_key, mock_stats_data_id):
        """Test limit parameter handling."""
        # Test with limit > LIMIT (100000)
        reader = StatsDataReader(
            api_key=mock_api_key,
            statsDataId=mock_stats_data_id,
            limit=200000
        )
        
        assert reader.limit == 100000  # Should be capped at LIMIT
        assert reader.max == 200000     # max should store original value
        
        # Test with limit <= LIMIT
        reader2 = StatsDataReader(
            api_key=mock_api_key,
            statsDataId=mock_stats_data_id,
            limit=50000
        )
        
        assert reader2.limit == 50000
        assert reader2.max is None
    
    # === メタデータテスト ===
    
    @patch('jpy_datareader.base._BaseReader._get_response')
    def test_class_name_mapping(self, mock_get_response, stats_reader, expected_data_json):
        """Test CLASS_NAME_MAPPING functionality."""
        self._setup_mock_response(mock_get_response, expected_data_json)
        stats_reader.read()
        
        assert hasattr(stats_reader, 'CLASS_NAME_MAPPING')
        expected_mapping = {
            'tab': '表章項目',
            'cat01': '用途分類',
            'cat02': '世帯区分',
            'cat03': '世帯主の年齢階級',
            'area': '地域区分',
            'time': '時間軸（月次）'
        }
        assert stats_reader.CLASS_NAME_MAPPING == expected_mapping
    
    # === 異常系・エラーハンドリングテスト ===
    
    @patch('jpy_datareader.base._BaseReader._get_response')
    def test_api_error_status_non_zero(self, mock_get_response, stats_reader):
        """Test handling of API error status (STATUS != 0)."""
        error_responses = [
            {
                "GET_STATS_DATA": {
                    "RESULT": {
                        "STATUS": 100,
                        "ERROR_MSG": "統計表が見つかりません。",
                        "DATE": "2025-07-13T10:30:00.000+09:00"
                    }
                }
            },
            {
                "GET_STATS_DATA": {
                    "RESULT": {
                        "STATUS": 110,
                        "ERROR_MSG": "該当するデータが存在しません。",
                        "DATE": "2025-07-13T10:30:00.000+09:00"
                    }
                }
            }
        ]
        
        for error_response in error_responses:
            self._setup_mock_response(mock_get_response, error_response)
            
            result = stats_reader.read_json()
            
            assert result == error_response
            assert stats_reader.STATUS == error_response["GET_STATS_DATA"]["RESULT"]["STATUS"]
            assert stats_reader.ERROR_MSG == error_response["GET_STATS_DATA"]["RESULT"]["ERROR_MSG"]
    
    @patch('jpy_datareader.base._BaseReader._get_response')
    def test_http_errors(self, mock_get_response, stats_reader):
        """Test handling of various HTTP errors."""
        errors = [
            ("Unable to read URL: https://api.e-stat.go.jp/rest/3.0/app/json/getStatsData", "Unable to read URL"),
            ("Unable to read URL: server error 500", "server error 500"),
            ("Unable to read URL: timeout", "timeout"),
            ("Unable to read URL: connection failed", "connection failed")
        ]
        
        for error_msg, match_pattern in errors:
            mock_get_response.side_effect = RemoteDataError(error_msg)
            
            with pytest.raises(RemoteDataError, match=match_pattern):
                stats_reader.read()
            
            # Reset for next iteration
            mock_get_response.side_effect = None
    

    # === エッジケーステスト ===
    
    @patch('jpy_datareader.base._BaseReader._get_response')
    def test_empty_or_malformed_class_obj_handling(self, mock_get_response, stats_reader):
        """Test handling of empty or malformed CLASS_OBJ."""
        test_cases = [
            {"CLASS_OBJ": []},  # Empty CLASS_OBJ
            {"CLASS_OBJ": "not_a_list"},  # Malformed CLASS_OBJ
            {"CLASS_OBJ": None}  # None CLASS_OBJ
        ]
        
        for class_inf in test_cases:
            response = {
                "GET_STATS_DATA": {
                    "RESULT": {"STATUS": 0},
                    "STATISTICAL_DATA": {
                        "RESULT_INF": {"TOTAL_NUMBER": 1},
                        "TABLE_INF": {"STATISTICS_NAME": "テスト"},
                        "CLASS_INF": class_inf,
                        "DATA_INF": {
                            "VALUE": [{"@tab": "01", "@unit": "一万分比", "$": "1000"}]
                        }
                    }
                }
            }
            
            self._setup_mock_response(mock_get_response, response)
            
            # Should handle gracefully without crashing
            result_df = stats_reader.read()
            assert isinstance(result_df, pd.DataFrame)
            assert len(result_df) == 1
    
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
            }
        }
        
        self._setup_mock_response(mock_get_response, incomplete_response)
        
        # KeyErrorが発生することを期待するか、適切なエラーハンドリングを確認
        with pytest.raises(KeyError):
            stats_reader.read()
    
    @patch('jpy_datareader.base._BaseReader._get_response')
    def test_missing_value_key(self, mock_get_response, stats_reader):
        """Test handling of missing VALUE key in DATA_INF."""
        incomplete_response = {
            "GET_STATS_DATA": {
                "RESULT": {"STATUS": 0},
                "STATISTICAL_DATA": {
                    "RESULT_INF": {"TOTAL_NUMBER": 0},
                    "TABLE_INF": {"STATISTICS_NAME": "テスト"},
                    "DATA_INF": {}
                }
            }
        }
        
        self._setup_mock_response(mock_get_response, incomplete_response)
        
        # KeyErrorが発生することを期待
        with pytest.raises(KeyError):
            stats_reader.read()
        
    @patch('jpy_datareader.base._BaseReader._get_response')
    def test_missing_class_inf_key(self, mock_get_response, stats_reader):
        """Test handling of missing CLASS_INF key."""
        response_without_class_inf = {
            "GET_STATS_DATA": {
                "RESULT": {"STATUS": 0},
                "STATISTICAL_DATA": {
                    "RESULT_INF": {"TOTAL_NUMBER": 1},
                    "TABLE_INF": {"STATISTICS_NAME": "テスト"},
                    # Missing CLASS_INF
                    "DATA_INF": {
                        "VALUE": [{"@tab": "01", "@unit": "一万分比", "$": "1000"}]
                    }
                }
            }
        }
        
        self._setup_mock_response(mock_get_response, response_without_class_inf)
        
        # Should handle gracefully
        result = stats_reader.read()
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
    
    @patch('jpy_datareader.base._BaseReader._get_response')
    def test_malformed_value_entries(self, mock_get_response, stats_reader):
        """Test handling of malformed VALUE entries."""
        response_malformed_values = {
            "GET_STATS_DATA": {
                "RESULT": {"STATUS": 0},
                "STATISTICAL_DATA": {
                    "RESULT_INF": {"TOTAL_NUMBER": 3},
                    "TABLE_INF": {"STATISTICS_NAME": "テスト"},
                    "CLASS_INF": {
                        "CLASS_OBJ": [{
                            "@id": "tab",
                            "@name": "表章項目",
                            "CLASS": [{"@code": "01", "@name": "項目1", "@level": "1"}]
                        }]
                    },
                    "DATA_INF": {
                        "VALUE": [
                            {"@tab": "01", "@unit": "一万分比", "$": "1000"},  # Normal entry
                            {"@tab": "01", "@unit": "一万分比"},  # Missing $ key
                            {"@unit": "一万分比", "$": "2000"}  # Missing @tab key
                        ]
                    }
                }
            }
        }
        
        self._setup_mock_response(mock_get_response, response_malformed_values)
        
        # Should handle gracefully even with malformed entries
        result = stats_reader.read()
        assert isinstance(result, pd.DataFrame)
        # The exact behavior depends on implementation, but it shouldn't crash
    
    @patch('jpy_datareader.base._BaseReader._get_response')
    def test_nested_class_structure_edge_cases(self, mock_get_response, stats_reader):
        """Test handling of edge cases in nested CLASS structure."""
        response_nested_edge_cases = {
            "GET_STATS_DATA": {
                "RESULT": {"STATUS": 0},
                "STATISTICAL_DATA": {
                    "RESULT_INF": {"TOTAL_NUMBER": 1},
                    "TABLE_INF": {"STATISTICS_NAME": "テスト"},
                    "CLASS_INF": {
                        "CLASS_OBJ": [
                            {
                                "@id": "tab",
                                "@name": "表章項目",
                                "CLASS": []  # Empty CLASS array
                            },
                            {
                                "@id": "cat01",
                                "@name": "用途分類",
                                "CLASS": [
                                    {"@code": "001", "@name": "食料"},  # Missing @level
                                    {"@code": "002", "@level": "1"}  # Missing @name
                                ]
                            }
                        ]
                    },
                    "DATA_INF": {
                        "VALUE": [{"@tab": "01", "@cat01": "001", "@unit": "一万分比", "$": "1000"}]
                    }
                }
            }
        }
        
        self._setup_mock_response(mock_get_response, response_nested_edge_cases)
        
        result = stats_reader.read()
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
    
    # === 内部メソッドテスト ===
    
    @patch('jpy_datareader.base._BaseReader._get_response')
    def test_statsjson_to_dataframe(self, mock_get_response, stats_reader, expected_data_json):
        """Test _statsjson_to_dataframe method."""
        self._setup_mock_response(mock_get_response, expected_data_json)
        
        json_data = stats_reader.read_json()
        value_df = stats_reader._statsjson_to_dataframe(json_data)
        
        assert isinstance(value_df, pd.DataFrame)
        assert len(value_df) == 4
        expected_columns = ['tab', 'cat01', 'cat02', 'cat03', 'area', 'time', 'unit', 'value']
        assert list(value_df.columns) == expected_columns
        
        # Verify category_columns attribute
        assert hasattr(stats_reader, 'category_columns')
        assert stats_reader.category_columns == ['tab', 'cat01', 'cat02', 'cat03', 'area', 'time', 'unit']
        
        # Verify data content
        assert value_df['value'].iloc[0] == "10000"
        assert value_df['tab'].iloc[0] == "01"
        assert value_df['unit'].iloc[0] == "一万分比"
    
    @patch('jpy_datareader.base._BaseReader._get_response')
    def test_handle_missing_values(self, mock_get_response, stats_reader, expected_data_json):
        """Test _handle_missing_values method."""
        self._setup_mock_response(mock_get_response, expected_data_json)
        
        json_data = stats_reader.read_json()
        value_df = stats_reader._statsjson_to_dataframe(json_data)
        value_df_handled = stats_reader._handle_missing_values(value_df, json_data)
        
        assert isinstance(value_df_handled, pd.DataFrame)
        assert pd.api.types.is_numeric_dtype(value_df_handled['value'])
        
        # Check that "-" was converted to NaN
        assert value_df['value'].iloc[3] == "-"  # Original value
        assert pd.isna(value_df_handled['value'].iloc[3])  # Should be NaN after handling
        
        # Check that numeric values are preserved
        assert value_df_handled['value'].iloc[0] == 10000.0
        assert value_df_handled['value'].iloc[1] == 33.0
        assert value_df_handled['value'].iloc[2] == 29.0
    
    @patch('jpy_datareader.base._BaseReader._get_response')
    def test_handle_missing_values_with_multiple_notes(self, mock_get_response, stats_reader):
        """Test _handle_missing_values method with multiple NOTE characters."""
        response_multiple_notes = {
            "GET_STATS_DATA": {
                "RESULT": {"STATUS": 0},
                "STATISTICAL_DATA": {
                    "RESULT_INF": {"TOTAL_NUMBER": 4},
                    "TABLE_INF": {"STATISTICS_NAME": "テスト"},
                    "CLASS_INF": {
                        "CLASS_OBJ": [{
                            "@id": "tab",
                            "@name": "表章項目",
                            "CLASS": [{"@code": "01", "@name": "項目1", "@level": "1"}]
                        }]
                    },
                    "DATA_INF": {
                        "NOTE": [
                            {"@char": "-", "$": "データなし"},
                            {"@char": "***", "$": "秘匿"},
                            {"@char": "...", "$": "計算不能"}
                        ],
                        "VALUE": [
                            {"@tab": "01", "@unit": "一万分比", "$": "1000"},
                            {"@tab": "01", "@unit": "一万分比", "$": "-"},
                            {"@tab": "01", "@unit": "一万分比", "$": "***"},
                            {"@tab": "01", "@unit": "一万分比", "$": "..."}
                        ]
                    }
                }
            }
        }
        
        self._setup_mock_response(mock_get_response, response_multiple_notes)
        
        json_data = stats_reader.read_json()
        value_df = stats_reader._statsjson_to_dataframe(json_data)
        value_df_handled = stats_reader._handle_missing_values(value_df, json_data)
        
        assert value_df_handled['value'].iloc[0] == 1000.0  # Normal value
        assert pd.isna(value_df_handled['value'].iloc[1])  # "-" converted to NaN
        assert pd.isna(value_df_handled['value'].iloc[2])  # "***" converted to NaN
        assert pd.isna(value_df_handled['value'].iloc[3])  # "..." converted to NaN
    
    @patch('jpy_datareader.base._BaseReader._get_response')
    def test_merge_class_metadata(self, mock_get_response, stats_reader, expected_data_json):
        """Test _merge_class_metadata method."""
        self._setup_mock_response(mock_get_response, expected_data_json)
        
        json_data = stats_reader.read_json()
        value_df = stats_reader._statsjson_to_dataframe(json_data)
        value_df_handled = stats_reader._handle_missing_values(value_df, json_data)
        value_df_merged = stats_reader._merge_class_metadata(value_df_handled, json_data)
        
        assert isinstance(value_df_merged, pd.DataFrame)
        
        # Check that codes have been renamed with _code suffix
        expected_code_columns = ['tab_code', 'cat01_code', 'cat02_code', 'cat03_code', 'area_code', 'time_code']
        for col in expected_code_columns:
            assert col in value_df_merged.columns
        
        assert 'unit' in value_df_merged.columns  # unit stays as is
        assert 'value' in value_df_merged.columns
        
        # Check that metadata columns have been added
        expected_metadata_columns = ['tab_name', 'tab_level', 'cat01_name', 'cat01_level', 'cat01_unit', 'cat01_parentCode']
        for col in expected_metadata_columns:
            assert col in value_df_merged.columns
        
        # Verify metadata content
        assert value_df_merged['tab_name'].iloc[0] == "支出金額"
        assert value_df_merged['cat01_name'].iloc[0] == "食料"
        assert value_df_merged['cat01_unit'].iloc[0] == "円"
    
    @patch('jpy_datareader.base._BaseReader._get_response')
    def test_merge_class_metadata_with_missing_attributes(self, mock_get_response, stats_reader):
        """Test _merge_class_metadata method with missing class attributes."""
        response_missing_attrs = {
            "GET_STATS_DATA": {
                "RESULT": {"STATUS": 0},
                "STATISTICAL_DATA": {
                    "RESULT_INF": {"TOTAL_NUMBER": 1},
                    "TABLE_INF": {"STATISTICS_NAME": "テスト"},
                    "CLASS_INF": {
                        "CLASS_OBJ": [{
                            "@id": "tab",
                            "@name": "表章項目",
                            "CLASS": [
                                {"@code": "01", "@name": "項目1"},  # Missing @level, @unit, @parentCode
                                {"@code": "02", "@level": "1"}     # Missing @name
                            ]
                        }]
                    },
                    "DATA_INF": {
                        "VALUE": [{"@tab": "01", "@unit": "一万分比", "$": "1000"}]
                    }
                }
            }
        }
        
        self._setup_mock_response(mock_get_response, response_missing_attrs)
        
        json_data = stats_reader.read_json()
        value_df = stats_reader._statsjson_to_dataframe(json_data)
        value_df_handled = stats_reader._handle_missing_values(value_df, json_data)
        value_df_merged = stats_reader._merge_class_metadata(value_df_handled, json_data)
        
        assert isinstance(value_df_merged, pd.DataFrame)
        assert 'tab_name' in value_df_merged.columns
        assert 'tab_level' in value_df_merged.columns
        
        # Should handle missing attributes gracefully
        # (exact behavior depends on implementation)
    
    @patch('jpy_datareader.base._BaseReader._get_response')
    def test_apply_colname_transformations(self, mock_get_response, stats_reader, expected_data_json):
        """Test _apply_colname_transformations method."""
        self._setup_mock_response(mock_get_response, expected_data_json)
        
        json_data = stats_reader.read_json()
        value_df = stats_reader._statsjson_to_dataframe(json_data)
        value_df_handled = stats_reader._handle_missing_values(value_df, json_data)
        value_df_merged = stats_reader._merge_class_metadata(value_df_handled, json_data)
        value_df_transformed = stats_reader._apply_colname_transformations(value_df_merged, json_data)
        
        assert isinstance(value_df_transformed, pd.DataFrame)
        
        # Check Japanese column names (when lang != "E")
        expected_columns = [
            '表章項目コード', '用途分類コード', '世帯区分コード', '世帯主の年齢階級コード',
            '地域区分コード', '時間軸（月次）コード', '単位', '値',
            '表章項目', '表章項目階層レベル', '用途分類', '用途分類階層レベル',
            '用途分類単位', '用途分類親コード', '世帯区分', '世帯区分階層レベル',
            '世帯主の年齢階級', '世帯主の年齢階級階層レベル', '地域区分', '地域区分階層レベル',
            '時間軸（月次）', '時間軸（月次）階層レベル'
        ]
        
        for col in expected_columns:
            assert col in value_df_transformed.columns
        
        # Check tab_colname attribute
        assert hasattr(stats_reader, 'tab_colname')
        assert stats_reader.tab_colname == '表章項目'
    
    @patch('jpy_datareader.base._BaseReader._get_response')
    def test_apply_colname_transformations_english(self, mock_get_response, mock_api_key, mock_stats_data_id, expected_data_json):
        """Test _apply_colname_transformations method with English language setting."""
        # Create reader with English language
        en_reader = StatsDataReader(
            api_key=mock_api_key,
            statsDataId=mock_stats_data_id,
            lang="E"
        )
        
        self._setup_mock_response(mock_get_response, expected_data_json)
        
        json_data = en_reader.read_json()
        value_df = en_reader._statsjson_to_dataframe(json_data)
        value_df_handled = en_reader._handle_missing_values(value_df, json_data)
        value_df_merged = en_reader._merge_class_metadata(value_df_handled, json_data)
        value_df_transformed = en_reader._apply_colname_transformations(value_df_merged, json_data)
        
        assert isinstance(value_df_transformed, pd.DataFrame)
        
        # Check English column names remain as _code format
        expected_english_columns = [
            'tab_code', 'cat01_code', 'cat02_code', 'cat03_code',
            'area_code', 'time_code', 'unit', 'value',
            'tab_name', 'tab_level', 'cat01_name', 'cat01_level'
        ]
        
        for col in expected_english_columns:
            assert col in value_df_transformed.columns
        
        # Check tab_colname attribute for English
        assert hasattr(en_reader, 'tab_colname')
        assert en_reader.tab_colname == 'tab_name'


    # === 包括的統合テスト ===

    @patch('jpy_datareader.base._BaseReader._get_response')
    def test_complete_pipeline_consistency(self, mock_get_response, stats_reader, expected_data_json):
        """Test consistency of the complete data processing pipeline."""
        self._setup_mock_response(mock_get_response, expected_data_json)
        
        # Test complete pipeline through read() method
        final_result = stats_reader.read()
        
        # Test step-by-step pipeline
        json_data = stats_reader.read_json()
        step1 = stats_reader._statsjson_to_dataframe(json_data)
        step2 = stats_reader._handle_missing_values(step1, json_data)
        step3 = stats_reader._merge_class_metadata(step2, json_data)
        step4 = stats_reader._apply_colname_transformations(step3, json_data)
        
        # Compare final results
        assert len(final_result) == len(step4)
        
        # Value consistency (both should have same non-NaN values)
        final_values = final_result['値'].tolist()
        step4_values = step4['値'].tolist()
        
        for i in range(len(final_values)):
            if pd.isna(final_values[i]) and pd.isna(step4_values[i]):
                continue  # Both NaN
            assert final_values[i] == step4_values[i]

    @patch('jpy_datareader.base._BaseReader._get_response')
    def test_class_metadata_consistency(self, mock_get_response, stats_reader, expected_data_json):
        """Test consistency of class metadata merging."""
        self._setup_mock_response(mock_get_response, expected_data_json)
        
        json_data = stats_reader.read_json()
        result_df = stats_reader.read()
        
        # Verify that codes match expected class definitions
        class_objs = expected_data_json["GET_STATS_DATA"]["STATISTICAL_DATA"]["CLASS_INF"]["CLASS_OBJ"]
        
        # Find tab class and verify mapping
        tab_class = next(co for co in class_objs if co["@id"] == "tab")
        tab_classes = {cls["@code"]: cls["@name"] for cls in tab_class["CLASS"]}
        
        for idx, row in result_df.iterrows():
            tab_code = row['表章項目コード']
            tab_name = row['表章項目']
            assert tab_name == tab_classes[tab_code]
        
        # Verify cat01 class and verify mapping
        cat01_class = next(co for co in class_objs if co["@id"] == "cat01")
        cat01_classes = {cls["@code"]: cls["@name"] for cls in cat01_class["CLASS"]}
        
        for idx, row in result_df.iterrows():
            cat01_code = row['用途分類コード']
            cat01_name = row['用途分類']
            assert cat01_name == cat01_classes[cat01_code]

    @patch('jpy_datareader.base._BaseReader._get_response')
    def test_unit_consistency_across_processing(self, mock_get_response, stats_reader, expected_data_json):
        """Test that unit values remain consistent throughout processing."""
        self._setup_mock_response(mock_get_response, expected_data_json)
        
        json_data = stats_reader.read_json()
        
        # Extract expected unit values from raw data
        raw_units = [v["@unit"] for v in json_data["GET_STATS_DATA"]["STATISTICAL_DATA"]["DATA_INF"]["VALUE"]]
        expected_units = ["一万分比"] * 4
        assert raw_units == expected_units
        
        # Check units after each processing step
        value_df = stats_reader._statsjson_to_dataframe(json_data)
        assert value_df['unit'].tolist() == expected_units
        
        value_df_handled = stats_reader._handle_missing_values(value_df, json_data)
        assert value_df_handled['unit'].tolist() == expected_units
        
        value_df_merged = stats_reader._merge_class_metadata(value_df_handled, json_data)
        assert value_df_merged['unit'].tolist() == expected_units
        
        # Final result
        result_df = stats_reader.read()
        assert result_df['単位'].tolist() == expected_units

    @patch('jpy_datareader.base._BaseReader._get_response')
    def test_column_evolution_consistency(self, mock_get_response, stats_reader, expected_data_json):
        """Test that columns evolve correctly through processing steps."""
        self._setup_mock_response(mock_get_response, expected_data_json)
        
        json_data = stats_reader.read_json()
        
        # Step 1: Initial columns from _statsjson_to_dataframe
        value_df = stats_reader._statsjson_to_dataframe(json_data)
        expected_initial = ['tab', 'cat01', 'cat02', 'cat03', 'area', 'time', 'unit', 'value']
        assert list(value_df.columns) == expected_initial
        
        # Step 2: After _handle_missing_values (columns unchanged)
        value_df_handled = stats_reader._handle_missing_values(value_df, json_data)
        assert list(value_df_handled.columns) == expected_initial
        
        # Step 3: After _merge_class_metadata (columns expanded with metadata)
        value_df_merged = stats_reader._merge_class_metadata(value_df_handled, json_data)
        
        # Code columns should have _code suffix (except unit)
        code_columns = [col for col in value_df_merged.columns if col.endswith('_code')]
        expected_code_cols = ['tab_code', 'cat01_code', 'cat02_code', 'cat03_code', 'area_code', 'time_code']
        assert all(col in code_columns for col in expected_code_cols)
        
        # Metadata columns should be present
        metadata_columns = [col for col in value_df_merged.columns if col.endswith('_name') or col.endswith('_level')]
        assert len(metadata_columns) > 0
        assert 'tab_name' in value_df_merged.columns
        assert 'tab_level' in value_df_merged.columns
        
        # Step 4: After _apply_colname_transformations (Japanese column names)
        value_df_final = stats_reader._apply_colname_transformations(value_df_merged, json_data)
        
        # Should have Japanese column names
        japanese_columns = ['表章項目コード', '用途分類コード', '値', '表章項目']
        for col in japanese_columns:
            assert col in value_df_final.columns

    @patch('jpy_datareader.base._BaseReader._get_response')
    def test_error_resilience_in_internal_methods(self, mock_get_response, stats_reader):
        """Test error resilience in internal methods with corrupted data."""
        corrupted_data = {
            "GET_STATS_DATA": {
                "RESULT": {"STATUS": 0},
                "STATISTICAL_DATA": {
                        "RESULT_INF": {"TOTAL_NUMBER": 2},
                        "TABLE_INF": {"STATISTICS_NAME": "テスト"},
                        "CLASS_INF": "corrupted_string_instead_of_dict",
                        "DATA_INF": {
                            "VALUE": [
                                {"@tab": "01", "@unit": "円", "$": "1000"},
                                {"corrupted_key": "corrupted_value"}
                            ]
                        }
                }
            }
        }
        
        self._setup_mock_response(mock_get_response, corrupted_data)
        
        # Should not crash even with corrupted data
        try:
            result = stats_reader.read()
            assert isinstance(result, pd.DataFrame)
        except Exception as e:
            # If it does raise an exception, it should be a known type
            assert isinstance(e, (KeyError, AttributeError, ValueError, TypeError))

    @patch('jpy_datareader.base._BaseReader._get_response')
    def test_memory_efficiency_with_large_mock_dataset(self, mock_get_response, stats_reader):
        """Test memory efficiency with a larger mock dataset."""
        # Create a larger mock dataset (1000 records)
        large_values = []
        for i in range(1000):
            large_values.append({
                "@tab": f"{i % 10:02d}",
                "@cat01": f"{i % 100:03d}",
                "@cat02": f"{i % 5:02d}",
                "@cat03": f"{i % 3:02d}",
                "@area": f"{i % 47:05d}",
                "@time": f"2024{i % 12 + 1:03d}000",
                "@unit": "一万分比",
                "$": str(i * 100) if i % 50 != 0 else "-"  # Include some missing values
            })
        
        large_dataset = {
            "GET_STATS_DATA": {
                "RESULT": {"STATUS": 0},
                "STATISTICAL_DATA": {
                        "RESULT_INF": {"TOTAL_NUMBER": 1000},
                        "TABLE_INF": {"STATISTICS_NAME": "大規模テスト"},
                        "CLASS_INF": {
                            "CLASS_OBJ": [{
                                "@id": "tab",
                                "@name": "表章項目",
                                "CLASS": [{"@code": f"{i:02d}", "@name": f"項目{i}", "@level": "1"} for i in range(10)]
                            }]
                        },
                        "DATA_INF": {
                            "NOTE": [{"@char": "-", "$": "データなし"}],
                            "VALUE": large_values
                        }
                }
            }
        }
        
        self._setup_mock_response(mock_get_response, large_dataset)
        
        # Should handle large dataset efficiently
        result = stats_reader.read()
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1000
        
        # Verify that missing values are handled correctly
        missing_count = result['値'].isna().sum()
        assert missing_count == 20  # 1000 / 50 = 20 missing values

    @patch('jpy_datareader.base._BaseReader._get_response')
    def test_data_type_consistency_across_pipeline(self, mock_get_response, stats_reader, expected_data_json):
        """Test that data types remain consistent across the processing pipeline."""
        self._setup_mock_response(mock_get_response, expected_data_json)
        
        json_data = stats_reader.read_json()
        
        # Step 1: Check initial data types
        step1 = stats_reader._statsjson_to_dataframe(json_data)
        assert step1['value'].dtype == 'object'  # Initially strings
        assert step1['tab'].dtype == 'object'
        assert step1['unit'].dtype == 'object'
        
        # Step 2: After handling missing values
        step2 = stats_reader._handle_missing_values(step1, json_data)
        assert pd.api.types.is_numeric_dtype(step2['value'])  # Should be numeric now
        assert step2['tab'].dtype == 'object'  # Codes remain strings
        assert step2['unit'].dtype == 'object'
        
        # Step 3: After merging metadata
        step3 = stats_reader._merge_class_metadata(step2, json_data)
        assert pd.api.types.is_numeric_dtype(step3['value'])
        assert step3['tab_code'].dtype == 'object'
        assert step3['tab_name'].dtype == 'object'
        assert step3['unit'].dtype == 'object'
        
        # Step 4: Final transformation
        step4 = stats_reader._apply_colname_transformations(step3, json_data)
        assert pd.api.types.is_numeric_dtype(step4['値'])
        assert step4['表章項目コード'].dtype == 'object'
        assert step4['表章項目'].dtype == 'object'
        assert step4['単位'].dtype == 'object'

    @patch('jpy_datareader.base._BaseReader._get_response')
    def test_end_to_end_split_by_unit_consistency(self, mock_get_response, stats_reader):
        """Test end-to-end consistency of split_by_unit functionality."""
        multi_unit_data = {
            "GET_STATS_DATA": {
                "RESULT": {"STATUS": 0},
                "STATISTICAL_DATA": {
                        "RESULT_INF": {"TOTAL_NUMBER": 6},
                        "TABLE_INF": {"STATISTICS_NAME": "テスト", "OVERALL_TOTAL_NUMBER": 6},
                        "CLASS_INF": {
                            "CLASS_OBJ": [{
                                "@id": "tab",
                                "@name": "表章項目",
                                "CLASS": [
                                        {"@code": "01", "@name": "項目1", "@level": "1"},
                                        {"@code": "02", "@name": "項目2", "@level": "1"}
                                ]
                            }]
                        },
                        "DATA_INF": {
                            "VALUE": [
                                {"@tab": "01", "@unit": "円", "$": "1000"},
                                {"@tab": "01", "@unit": "円", "$": "2000"},
                                {"@tab": "02", "@unit": "千人", "$": "100"},
                                {"@tab": "02", "@unit": "千人", "$": "200"},
                                {"@tab": "01", "@unit": "％", "$": "50"},
                                {"@tab": "02", "@unit": "％", "$": "75"}
                            ]
                        }
                }
            }
        }
        
        self._setup_mock_response(mock_get_response, multi_unit_data)
        
        # Test regular read
        regular_result = stats_reader.read()
        assert len(regular_result) == 6
        assert set(regular_result['単位'].unique()) == {'円', '千人', '％'}
        
        # Reset mock for split by unit test
        self._setup_mock_response(mock_get_response, multi_unit_data)
        
        # Test split by units
        split_result = stats_reader.read(split_by_unit=True)
        
        assert isinstance(split_result, dict)
        assert set(split_result.keys()) == {'円', '千人', '％'}
        
        # Verify data integrity and consistency
        total_rows = sum(len(df) for df in split_result.values())
        assert total_rows == 6  # Should equal original dataset
        
        # Verify each unit group
        yen_data = split_result['円']
        assert len(yen_data) == 2
        assert all(yen_data['単位'] == '円')
        assert set(yen_data['値'].tolist()) == {1000.0, 2000.0}
        
        sen_nin_data = split_result['千人']
        assert len(sen_nin_data) == 2
        assert all(sen_nin_data['単位'] == '千人')
        assert set(sen_nin_data['値'].tolist()) == {100.0, 200.0}
        
        percent_data = split_result['％']
        assert len(percent_data) == 2
        assert all(percent_data['単位'] == '％')
        assert set(percent_data['値'].tolist()) == {50.0, 75.0}
        
        # Verify that combining splits gives original data (order-independent comparison)
        combined = pd.concat(split_result.values(), ignore_index=True)
        
        # Compare sorted values to ensure consistency
        combined_sorted_values = sorted(combined['値'].tolist())
        regular_sorted_values = sorted(regular_result['値'].tolist())
        assert combined_sorted_values == regular_sorted_values



