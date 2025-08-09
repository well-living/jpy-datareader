import pytest
import pandas as pd

from jpy_datareader.estat import DataCatalogReader, _BASE_URL

# -------------------------------------------------------------------
# ヘルパ: ダミーHTTPレスポンス
# -------------------------------------------------------------------

class _DummyResponse:
    def __init__(self, payload):
        self._payload = payload
    def json(self):
        return self._payload

def _make_catalog_json(items, *, status="0", err=None, date="2025-06-01",
                       lang="J", data_format="JSON", next_key=None):
    """e-Stat getDataCatalog の最小JSONを合成"""
    return {
        "GET_DATA_CATALOG": {
            "RESULT": {"STATUS": status, "ERROR_MSG": err, "DATE": date},
            "PARAMETER": {"LANG": lang, "DATA_FORMAT": data_format},
            "DATA_CATALOG_LIST_INF": {
                "NUMBER": len(items),
                "RESULT_INF": {
                    "FROM_NUMBER": 1 if items else 0,
                    "TO_NUMBER": len(items),
                    "NEXT_KEY": next_key,
                },
                "DATA_CATALOG_INF": items,
            },
        }
    }

# -------------------------------------------------------------------
# 基本: url が正しいか
# -------------------------------------------------------------------

def test_url_is_correct():
    rdr = DataCatalogReader(api_key="DUMMY")
    assert rdr.url == f"{_BASE_URL}/getDataCatalog?"

# -------------------------------------------------------------------
# params: 正常にバリデーションを通過する値が含まれること
# -------------------------------------------------------------------

def test_params_include_only_valid_values():
    rdr = DataCatalogReader(
        api_key="DUMMY",
        explanationGetFlg="Y",
        surveyYears=2023,
        openYears="2022-2024",
        statsField=1,
        statsCode="00200502",
        searchWord="人口",
        collectArea=2,            # 1..3 のみ有効
        dataType="CSV",           # 許可: ["XLS","CSV","PDF","XML","XLS_REP","DB"]
        startPosition=1,
        catalogId=12345,
        resourceId="67890",
        updatedDate="20250101",
    )
    p = rdr.params
    # 必須
    assert p["appId"] == "DUMMY"
    # 期待されるキー
    assert p["explanationGetFlg"] == "Y"
    assert p["surveyYears"] == 2023
    assert p["openYears"] == "2022-2024"
    assert p["statsField"] == 1
    assert p["statsCode"] == "00200502"
    assert p["searchWord"] == "人口"
    assert p["collectArea"] == 2
    assert p["dataType"] == "CSV"
    assert p["startPosition"] == 1
    assert p["catalogId"] == 12345
    assert p["resourceId"] == "67890"
    assert p["updatedDate"] == "20250101"
    # コンストラクタにあるが params には含めない属性（limit 等）
    assert "limit" not in p

# -------------------------------------------------------------------
# params: 不正値は除外されること
# -------------------------------------------------------------------

@pytest.mark.parametrize(
    "kwargs, excluded_keys",
    [
        ({"collectArea": 0}, ["collectArea"]),        # 1..3 以外は除外
        ({"dataType": "XLSX"}, ["dataType"]),         # 許可外
        ({"explanationGetFlg": "Z"}, ["explanationGetFlg"]),  # "Y"/"N"のみ
        ({"statsField": [1]}, ["statsField"]),        # 型不正
    ],
)
def test_params_exclude_invalid_values(kwargs, excluded_keys):
    rdr = DataCatalogReader(api_key="DUMMY", **kwargs)
    p = rdr.params
    for k in excluded_keys:
        assert k not in p

# -------------------------------------------------------------------
# _read_one_data: HTTP呼び出しが正しいURL/paramsで行われ、
#                 DataFrame列名が正しく正規化され、メタ属性が保存されること
# -------------------------------------------------------------------

def test_read_one_data_normalization_and_metadata(monkeypatch):
    # '@' 除去 / 末尾 '_$' 削り落とし の確認用キーを用意
    items = [
        {
            "@id": "ABC",
            "TITLE_$": "Foo Title",
            "STATISTICS_NAME": "Pop Stats",
            "LINK_$": "https://example.com",
            "RESOURCE_INF_id": 123,  # 変更なし
        }
    ]
    payload = _make_catalog_json(items)

    captured = {}
    def fake_get_response(self, url, params):
        captured["url"] = url
        captured["params"] = params
        return _DummyResponse(payload)

    monkeypatch.setattr(DataCatalogReader, "_get_response", fake_get_response)

    rdr = DataCatalogReader(
        api_key="DUMMY",
        explanationGetFlg="Y",
        searchWord="人口"
    )

    df = rdr._read_one_data(rdr.url, rdr.params)

    # 呼び出し引数の検証
    assert captured["url"] == rdr.url
    assert captured["params"] == rdr.params

    # DataFrame の内容検証（列名正規化）
    assert isinstance(df, pd.DataFrame)
    assert set(df.columns) >= {"id", "TITLE", "STATISTICS_NAME", "LINK", "RESOURCE_INF_id"}
    assert df.loc[0, "id"] == "ABC"
    assert df.loc[0, "TITLE"] == "Foo Title"
    assert df.loc[0, "STATISTICS_NAME"] == "Pop Stats"
    assert df.loc[0, "LINK"] == "https://example.com"
    assert df.loc[0, "RESOURCE_INF_id"] == 123

    # メタ属性の検証（_store_catalog_metadata の副作用）
    assert rdr.STATUS == "0"
    assert rdr.ERROR_MSG is None
    assert rdr.DATE == "2025-06-01"
    assert rdr.LANG == "J"
    assert rdr.DATA_FORMAT == "JSON"
    assert rdr.NUMBER == 1
    assert rdr.FROM_NUMBER == 1
    assert rdr.TO_NUMBER == 1
    assert rdr.NEXT_KEY is None

# -------------------------------------------------------------------
# _read_one_data: 複数件の正規化も問題ないこと
# -------------------------------------------------------------------

def test_read_one_data_multiple_items(monkeypatch):
    items = [
        {"@id": "ID1", "TITLE_$": "A", "LINK_$": "https://a.invalid"},
        {"@id": "ID2", "TITLE_$": "B", "LINK_$": "https://b.invalid"},
    ]
    payload = _make_catalog_json(items)

    def fake_get_response(self, url, params):
        return _DummyResponse(payload)

    monkeypatch.setattr(DataCatalogReader, "_get_response", fake_get_response)

    rdr = DataCatalogReader(api_key="DUMMY")
    df = rdr._read_one_data(rdr.url, rdr.params)

    assert len(df) == 2
    assert list(df["id"]) == ["ID1", "ID2"]
    assert list(df["TITLE"]) == ["A", "B"]
    assert list(df["LINK"]) == ["https://a.invalid", "https://b.invalid"]

# -------------------------------------------------------------------
# _read_one_data: 欠損気味のJSONでも .get(...) ベースで安全に属性が入ること
#                 （DataFrameは空でよい）
# -------------------------------------------------------------------

def test_read_one_data_missing_blocks(monkeypatch):
    # DATA_CATALOG_LIST_INF 自体はあるが DATA_CATALOG_INF を空に
    payload = _make_catalog_json([])

    def fake_get_response(self, url, params):
        return _DummyResponse(payload)

    monkeypatch.setattr(DataCatalogReader, "_get_response", fake_get_response)

    rdr = DataCatalogReader(api_key="DUMMY")
    df = rdr._read_one_data(rdr.url, rdr.params)

    # レコード0件
    assert isinstance(df, pd.DataFrame)
    assert df.empty

    # メタはちゃんと入っている
    assert rdr.NUMBER == 0
    assert rdr.FROM_NUMBER == 0
    assert rdr.TO_NUMBER == 0
    assert rdr.NEXT_KEY is None
