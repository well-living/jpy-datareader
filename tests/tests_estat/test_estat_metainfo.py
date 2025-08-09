# jpy-datareader/tests/tests_estat/test_estat_metainfo.py
# -*- coding: utf-8 -*-

import pytest
import pandas as pd
from unittest.mock import Mock, patch

# テスト対象のクラスをインポート
from jpy_datareader.estat import MetaInfoReader
from jpy_datareader._utils import RemoteDataError


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

