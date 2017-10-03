#!/usr/bin/env python

from pyclowder.utils import CheckMessage
from pyclowder.datasets import get_info, get_file_list, upload_metadata, download_metadata
from terrautils.extractors import TerrarefExtractor, build_metadata
from terrautils.geostreams import create_datapoint_with_dependencies
from terrautils.metadata import get_terraref_metadata, get_extractor_metadata, calculate_scan_time


# @begin extractor_sensor_position
# @in new_dataset_added

class Sensorposition2Geostreams(TerrarefExtractor):
	def __init__(self):
		super(Sensorposition2Geostreams, self).__init__()

		# parse command line and load default logging configuration
		self.setup(sensor='sensorposition')

	# Check whether dataset has geospatial metadata
	def check_message(self, connector, host, secret_key, resource, parameters):
		if 'dataset_info' in resource:
			ds_md = download_metadata(connector, host, secret_key, resource['dataset_info']['id'])

			terra_md = get_terraref_metadata(ds_md)
			ext_md = get_extractor_metadata(ds_md, self.extractor_info['name'])
			if terra_md and not ext_md:
				return CheckMessage.bypass

		return CheckMessage.ignore

	# Process the file and upload the results
	def process_message(self, connector, host, secret_key, resource, parameters):
		self.start_message()

		# @begin extract_positional_info_from_metadata
		# @in new_dataset_added
		# @out gantry_geometry
		# @end extract_positional_info

		ds_md = download_metadata(connector, host, secret_key, resource['dataset_info']['id'])
		terra_md = get_terraref_metadata(ds_md)

		# @begin upload_to_geostreams_API
		# @in sensor_position_in_lat/lon
		# @in sensor_id
		# @end upload_to_geostreams_API

		# Get sensor from datasetname
		(streamprefix, timestamp) = resource['dataset_info']['name'].split(' - ')
		date = timestamp.split("__")[0]
		scan_time = calculate_scan_time(terra_md)
		streamprefix += " Datasets"

		centroid = None
		bbox = None
		for entry in terra_md['spatial_metadata']:
			if 'centroid' in terra_md['spatial_metadata'][entry]:
				centroid = terra_md['spatial_metadata'][entry]['centroid']
			if 'bounding_box' in terra_md['spatial_metadata'][entry]:
				bbox = terra_md['spatial_metadata'][entry]['bounding_box']
		if centroid:
			dpmetadata = {
				"source_dataset": host + ("" if host.endswith("/") else "/") + \
								  "datasets/" + resource['dataset_info']['id'],
				"dataset_name": resource['dataset_info']['name']
			}
			create_datapoint_with_dependencies(connector, host, secret_key,
											   streamprefix, centroid,
											   scan_time, scan_time, dpmetadata, date, bbox)

		# Attach geometry to Clowder metadata as well
		ext_meta = build_metadata(host, self.extractor_info, resource['dataset_info']['id'], {
			"datapoints_added": 1
		}, 'dataset')
		upload_metadata(connector, host, secret_key, resource['dataset_info']['id'], ext_meta)

		self.end_message()

# @end extractor_sensor_position

if __name__ == "__main__":
	extractor = Sensorposition2Geostreams()
	extractor.start()
