# JPy-DataReader

![Python Version](https://img.shields.io/badge/python->=3.12-blue.svg)
![Version](https://img.shields.io/badge/version-0.1.0-green.svg)
![License](https://img.shields.io/badge/license-MIT-blue.svg)

Remote access to official data provided by the Government of Japan for use with pandas.

日本の政府統計の総合窓口 [e-Stat](https://www.e-stat.go.jp/) からデータを簡単に取得できるPythonライブラリです。

## Requirements

- Python >= 3.12
- numpy
- pandas
- requests
- python-dotenv

## Installation

Install using `pip`

``` shell
pip install jpy-datareader
```

## Quick Start

### APIキーの取得と設定

1. [e-Stat API](https://www.e-stat.go.jp/api/)でアプリケーションIDを取得
2. 環境変数として設定するか、コード内で直接指定

```python
# 環境変数での設定（推奨）
import os
from dotenv import load_dotenv

load_dotenv()
api_key = os.getenv("ESTAT_APP_ID")

# または直接指定
api_key = "Your_Application_ID"
```

### 基本的な使用方法

```python
import jpy_datareader.data as web

# 統計データの取得
df = web.DataReader("0003410379", "estat", api_key=api_key)
```

## 主要機能

### 1. 統計表一覧の検索

```python
from jpy_datareader.data import get_data_estat_statslist

# 基本的な統計表検索
statslist = get_data_estat_statslist(api_key=api_key, limit=10)

# キーワード検索
population_stats = get_data_estat_statslist(
    api_key=api_key,
    searchWord="人口",
    statsField="02",  # 人口・世帯分野
    limit=20
)
```

### 2. メタ情報の取得

```python
from jpy_datareader.data import get_data_estat_metainfo

# 統計表の構造情報を取得
metainfo = get_data_estat_metainfo(
    api_key=api_key,
    statsDataId="0003410379",
    has_lv_hierarchy=True  # 階層情報も取得
)
```

### 3. 統計データ取得

```python
from jpy_datareader.data import get_data_estat_statsdata

# 基本的なデータ取得
df = get_data_estat_statsdata(api_key=api_key, statsDataId="0003410379")

# 条件を指定したデータ取得
df_filtered = get_data_estat_statsdata(
    api_key=api_key,
    statsDataId="0003109558",
    cdArea="13000",  # 東京都のみ
    cdTimeFrom="2020",  # 2020年以降
    limit=10000
)
```


## 詳細な使用方法（Readerクラス）

より高度な制御や詳細な情報が必要な場合は、Readerクラスを直接使用できます：

### StatsListReader（統計表検索）

```python
from jpy_datareader.estat import StatsListReader

# 条件検索
statslist = StatsListReader(
    api_key=api_key,
    statsCode="00200521",  # 国勢調査
    surveyYears="2020-2023",
    searchWord="人口 AND 世帯"
)
df_list = statslist.read()

# JSON形式での取得
json_data = statslist.read_json()
```

### MetaInfoReader（メタ情報取得）

```python
from jpy_datareader.estat import MetaInfoReader

# 詳細なメタ情報取得
metainfo = MetaInfoReader(
    api_key=api_key,
    statsDataId="0003109558",
    has_lv_hierarchy=True,
    prefix_colname_with_classname=True
)

# 分類情報の取得
class_objects = metainfo.read_class_objs()
for class_data in class_objects:
    print(f"分類ID: {class_data['id']}")
    print(f"分類名: {class_data['name']}")
    print(f"項目数: {len(class_data['meta_dataframe'])}")
```

### StatsDataReader（統計データ取得）

```python
from jpy_datareader.estat import StatsDataReader

# 基本的な使用
statsdata = StatsDataReader(api_key=api_key, statsDataId="0003410379")
df = statsdata.read()

# 大量データの自動ページネーション
large_data = StatsDataReader(
    api_key=api_key,
    statsDataId="0003410379",
    limit=150000  # 10万件超でも自動処理
)
df_large = large_data.read()

# メタデータの取得
metadata = statsdata.get_metadata()
print(f"統計名: {statsdata.STAT_NAME}")
print(f"総データ件数: {statsdata.OVERALL_TOTAL_NUMBER}")
```

## 実践的なワークフロー例

1. `get_data_estat_statslist()` で興味のある統計表を探す
2. `get_data_estat_metainfo()` でデータ構造を把握
3. `get_data_estat_statsdata()` で実際のデータを取得
4. 取得したデータを分析・可視化

## 主要な特徴

- **自動ページネーション**: 10万件を超える大量データの自動分割取得
- **日本語対応**: カラム名の自動日本語化
- **階層データ処理**: 地域や産業分類などの階層構造に対応
- **条件絞り込み**: 地域、時期、分類による柔軟な絞り込み
- **エラーハンドリング**: 自動リトライとエラー処理
- **メタデータ管理**: 統計表の詳細情報を自動取得・保存

## パラメータ例

### 主要な絞り込み条件

```python
# 地域指定
cdArea="13000"        # 東京都
lvArea=2              # 都道府県レベル
cdAreaFrom="01000"    # 北海道から
cdAreaTo="47000"      # 沖縄県まで

# 時期指定
cdTime="2023"         # 2023年 ("2023000101"などで抽出する必要があるケースもあります)
cdTimeFrom="2020"     # 2020年以降
cdTimeTo="2023"       # 2023年まで

# 表章項目指定
cdTab="001"           # 特定の表章項目
lvTab=1               # 表章項目レベル1

# 統計分野
statsField="02"       # 人口・世帯
statsField="03"       # 労働・賃金
statsField="04"       # 農林水産業
```

## API仕様

政府統計の総合窓口（e-Stat）のAPI3.0版を使用しています。

詳細な仕様: https://www.e-stat.go.jp/api/api-info/e-stat-manual3-0

## クレジット

このサービスは、政府統計総合窓口(e-Stat)のAPI機能を使用していますが、サービスの内容は国によって保証されたものではありません。

https://www.e-stat.go.jp/api/api-info/credit

## ライセンス

MIT License

## 貢献

Issues や Pull Requests は歓迎します。

## サポート

- バグ報告: GitHub Issues
- 機能要望: GitHub Issues
- ドキュメント: 本READMEおよびサンプルコード

## gBizINFOのAPIについて

gBizINFOのAPIについて、2026年に新システム(v2)への移行に伴い、現行アカウント（トークン）の発行については25年9月をもって受付終了する予定と次期gBizINFOに関する情報ページに記載されています（2025/08/07現在）
https://info.gbiz.go.jp/html/R7Infomation.html

本ライブラリのgBizINFOからのデータ取得機能は非推奨です。
2026年にv2への移行後に、本ライブラリでもgBizINFOからのデータ取得を実装予定です。
