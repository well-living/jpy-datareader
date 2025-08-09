# jpy-datareader/tests/tests_estat/test_estat_statslist.py
# -*- coding: utf-8 -*-

import json
import pytest
import pandas as pd
from unittest.mock import Mock, patch

# テスト対象のクラスをインポート
from jpy_datareader.estat import StatsListReader
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
