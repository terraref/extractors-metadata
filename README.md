# Metadata extractors

This repository contains extractors that process and derive outputs from various sensor metadata. 

In cases like the sensorposition extractor, the same structure of positional metadata exists across multiple 
sensors so the extractor can accept a variety of datasets.


### Sensor position extractor
This extractor extracts positional data from the metadata into PostGIS geographies via the Clowder
Geostreams API, allowing for location-based searching. 

_Input_

  - Evaluation is triggered whenever new metadata is added to a dataset
  - Checks whether the metadata contains the following data structure:
```
    'lemnatec_measurement_metadata'
          'gantry_system_variable_metadata'
              'position x [m]'
              'position y [m]'
          'sensor_fixed_metadata'
              'location in camera box x [m]'
              'location in camera box y [m]'
              'field of view x [m]'
              'field of view y [m]'
```
  			
_Output_

  - A datapoint for the dataset will be generated in Clowder's Geostreams PostGIS database,
    containing GeoJSON describing the image's location.
  