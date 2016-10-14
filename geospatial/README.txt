Extractor files:
config.py
sensorposition.py
extractor_info.json
requirements.txt

Error estimation:
errorEstimation.py (current, the NW point will have (6,22) error)

Wrapped function:
fromGantry2LatLon.py (convert gantry poitions to Lat/Lon)



Running notes:
1. Set up pyclowder to version "bugfix/CATS-554-add-pyclowder-support-for-dataset"
2. use clowder-dev
3. confirm config.py
4. use test.json as test file (add the metadata for dataset. I used tool called Postman, a Chrome Plugin)