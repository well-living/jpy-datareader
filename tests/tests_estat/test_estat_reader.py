# jpy-datareader/tests/tests_estat/test_estat_reader.py
# -*- coding: utf-8 -*-

import pytest
import pandas as pd
from unittest.mock import patch

# テスト対象のクラスをインポート
from jpy_datareader.estat import _eStatReader, colname_to_japanese

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
    
    def test_api_key_from_environment():
        with patch('jpy_datareader.estat.DOTENV_AVAILABLE', False), \
            patch.dict('os.environ', {'E_STAT_API_KEY': 'env_test_key'}, clear=True):
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
