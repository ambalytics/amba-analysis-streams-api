# amba-analysis-streams-api
REST API for stream analysis platform

Monitoring over influxdb
- every response will be saved as:
```
 point = {
        "measurement": "response_time",
        "tags": {
            "path": request.url.path
        },
        "fields": {
            'response_time': int(process_time * 1000),
            'url': str(request.url)
        },
        "time": datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')}
```

- show api response count per 5m in range
```
from(bucket: "api_monitor")
  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
  |> filter(fn: (r) => r["_measurement"] == "response_time")
  |> filter(fn: (r) => r["_field"] == "response_time")
  |> group()
  |> aggregateWindow(every: 5m, fn: count, createEmpty: false)
  |> yield(name: "mean")
```