# pgfun

Some fun with pg
Depends on: Bash, Docker with postgres image pulled, Leiningen

## Usage

As of 1/22/18:

./run.sh

### To store

Accepts JSON with fields (all required)
| Name    | Description |
|---------|-------------|
| asset   | asset name  |
| date    | yyyy-MM-dd  |
| open    | open price  |
| high    | daily high  |
| low     | daily low   |
| close   | close price |

Storage example:

```
curl -i -XPOST -H 'Content-Type: application/json' -d '{"asset": "amz", "date": "2019-01-20", "open": 100, "high": 185, "low": 170, "close": 176}' http://localhost:3000/data/ohlc
```

### To retrieve

List the known assets
```
curl -i http://localhost:3000/data/ohlc
```

Retrieve aggregate information about a specific asset (full timerange):
```
curl -i http://localhost:3000/data/ohlc/<asset>
```

## License

Copyright © 2018 David Charubini

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
