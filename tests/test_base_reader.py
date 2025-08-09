

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

