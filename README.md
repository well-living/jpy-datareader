# JPy-DataReader

## Installation

Install using `pip`

``` shell
pip install jpy-datareader
```

## Usage

```Python
import jpy_datareader.data as web

api_key = "Your_Application_ID"
df = web.DataReader("0003109558", "estat", api_key=api_key)
```

### 政府統計の総合窓口（e-Stat）のAPI3.0版で統計データ取得

- 政府統計の総合窓口（e-Stat）のAPI3.0版の仕様

https://www.e-stat.go.jp/api/api-info/e-stat-manual3-0


``` python
import jpy_datareader as jdr

api_key = "Your_Application_ID"
jdr.get_data_estat_statsdata(api_key, statsDataId="0003109558")
```

```Python
from jpy_datareader import estat

statsdata = estat.StatsDataReader(api_key, statsDataId="0003109558")
df = statsdata.read()
```

### e-StatAPIで統計表情報取得
```Python
import jpy_datareader as jdr

api_key = "Your_Application_ID"
statslist = jdr.get_data_estat_statslist(api_key)
```

## クレジット
このサービスは、政府統計総合窓口(e-Stat)のAPI機能を使用していますが、サービスの内容は国によって保証されたものではありません。
https://www.e-stat.go.jp/api/api-info/credit
