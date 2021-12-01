# InfluxDB client for Erlang

InfluxDB client for Erlang (the new version). In progress...

- Implemented v2 InfluxDB API.
- Automation insights.
- Much more coming.

Start an InfluxDB instance:

```
# nohup docker run -p 8086:8086 \
      -v influxdb:/var/lib/influxdb \
	 -e INFLUXDB_HTTP_ENABLED=true \
	 -e INFLUXDB_HTTP_AUTH_ENABLED=false \
      influxdb:1.8.9 >influxdb.log 2>&1 &
```

How to build and test:

```
# make build
# make test
```

Contact us for more info: "Davait Systems" <info@davait.com>

