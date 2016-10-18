# Sensor position extractor

### Geostream environment
This extractor uses the current mapping in the Geostreams API:
- Geostreams sensor === A site, e.g. Maricopa
- Geostreams stream === A physical sensor instrument, e.g. flirIrCamera
- Geostreams datapoint === A single capture from that sensor, e.g. one timestamp or dataset in Clowder

### Required Geostreams Objects
**Creating the "sensor"**
At least one sensor (and stream) must exist before the extractor is started. This can be done in two ways:

1. Create in GUI using top toolbar (Sensors > Create)
  
2. Create using API

```
curl -X POST \
    -d '{"name":"Maricopa 2016","type":"point","geometry":{"type":"Point","coordinates":[-112.025920, 33.053788]}, "properties":{}}' \
    -H "Content-Type: application/json" \
    <CLOWDER_URL>/api/geostreams/sensors
```

**Streams per instrument**
The geostream_map in config.py defines the IDs of streams in Clowder associated with various instruments. 

The default mapping is:

```
    {
        "stereoTop": "101",
        "flirIr": "102",
        "co2Sensor": "103"
    }
```

If a sensor is not found in the mapping, "99" will be used.

If these stream IDs do not exist, the following cURL commands will create them:

```
curl -X POST {clowder_url}/api/geostreams/streams

```

### Error estimation
error_estimation.py (current, the NW point will have (6,22) error)