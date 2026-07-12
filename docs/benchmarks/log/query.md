# Query URL and payload for Elastic Search
## Count
URL: `http://127.0.0.1:9200/_count`

## Query by timerange
URL: `http://127.0.0.1:9200/_search`

You can use the following payload to get the full timerange first.
```JSON
{"size":0,"aggs":{"max_timestamp":{"max":{"field":"timestamp"}},"min_timestamp":{"min":{"field":"timestamp"}}}}
```

And then use this payload to query by timerange.
```JSON
{
  "from": 0,
  "size": 1000,
  "query": {
    "range": {
      "timestamp": {
        "gte": "2024-08-16T04:30:44.000Z",
        "lte": "2024-08-16T04:51:52.000Z"
      }
    }
  }
}
```

## Query by condition
URL: `http://127.0.0.1:9200/_search`
### Structured payload
```JSON
{
  "from": 0,
  "size": 10000,
  "query": {
    "bool": {
      "must": [
        {
          "term": {
            "user.keyword": "CrucifiX"
          }
        },
        {
          "term": {
            "method.keyword": "OPTION"
          }
        },
        {
          "term": {
            "path.keyword": "/user/booperbot124"
          }
        },
        {
          "term": {
            "http_version.keyword": "HTTP/1.1"
          }
        },
        {
          "term": {
            "status": "401"
          }
        }
      ]
    }
  }
}
```
### Unstructured payload
```JSON
{
  "from": 0,
  "size": 10000,
  "query": {
    "bool": {
      "must": [
        {
          "match_phrase": {
            "message": "CrucifiX"
          }
        },
        {
          "match_phrase": {
            "message": "OPTION"
          }
        },
        {
          "match_phrase": {
            "message": "/user/booperbot124"
          }
        },
        {
          "match_phrase": {
            "message": "HTTP/1.1"
          }
        },
        {
          "match_phrase": {
            "message": "401"
          }
        }
      ]
    }
  }
}
```

## Query by condition and timerange
URL: `http://127.0.0.1:9200/_search`
### Structured payload
```JSON
{
  "size": 10000,
  "query": {
    "bool": {
      "must": [
        {
          "term": {
            "user.keyword": "CrucifiX"
          }
        },
        {
          "term": {
            "method.keyword": "OPTION"
          }
        },
        {
          "term": {
            "path.keyword": "/user/booperbot124"
          }
        },
        {
          "term": {
            "http_version.keyword": "HTTP/1.1"
          }
        },
        {
          "term": {
            "status": "401"
          }
        },
        {
          "range": {
            "timestamp": {
              "gte": "2024-08-19T07:03:37.383Z",
              "lte": "2024-08-19T07:24:58.883Z"
            }
          }
        }
      ]
    }
  }
}
```
### Unstructured payload
```JSON
{
  "size": 10000,
  "query": {
    "bool": {
      "must": [
        {
          "match_phrase": {
            "message": "CrucifiX"
          }
        },
        {
          "match_phrase": {
            "message": "OPTION"
          }
        },
        {
          "match_phrase": {
            "message": "/user/booperbot124"
          }
        },
        {
          "match_phrase": {
            "message": "HTTP/1.1"
          }
        },
        {
          "match_phrase": {
            "message": "401"
          }
        },
        {
          "range": {
            "timestamp": {
              "gte": "2024-08-19T05:16:17.099Z",
              "lte": "2024-08-19T05:46:02.722Z"
            }
          }
        }
      ]
    }
  }
}
```
