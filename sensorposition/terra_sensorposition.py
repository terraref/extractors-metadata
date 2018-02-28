#!/usr/bin/env python

from pyclowder.utils import CheckMessage
from pyclowder.datasets import get_info, get_file_list, upload_metadata, download_metadata
from terrautils.extractors import TerrarefExtractor, build_metadata
from terrautils.geostreams import create_datapoint_with_dependencies, get_sensor_by_name, create_sensor, \
	get_stream_by_name, create_stream, create_datapoint
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
		if resource['type'] != "dataset":
			if 'name' not in resource:
				resource['name'] = resource["type"]
			self.log_skip(resource, "position is only logged for dataset metadata")
			return CheckMessage.ignore
		self.start_check(resource)

		if 'spatial_metadata' in resource['metadata']:
			ds_md = download_metadata(connector, host, secret_key, resource['id'])
			ext_md = get_extractor_metadata(ds_md, self.extractor_info['name'])
			if not ext_md:
				return CheckMessage.bypass
			else:
				self.log_skip(resource, "sensorposition metadata already exists")
				return CheckMessage.ignore
		else:
			self.log_skip(resource, "newly added metadata is not from LemnaTec")
			return CheckMessage.ignore

	# Process the file and upload the results
	def process_message(self, connector, host, secret_key, resource, parameters):
		self.start_message(resource)

		terra_md = resource['metadata']
		ds_info = get_info(connector, host, secret_key, resource['id'])

		# @begin extract_positional_info_from_metadata
		# @in new_dataset_added
		# @out gantry_geometry
		# @end extract_positional_info

		# Get sensor from datasetname
		self.log_info(resource, "Getting position information from metadata")
		(streamprefix, timestamp) = ds_info['name'].split(' - ')
		date = timestamp.split("__")[0]
		scan_time = calculate_scan_time(terra_md)
		streamprefix += " Datasets"
		dpmetadata = {
			"source_dataset": host + ("" if host.endswith("/") else "/") + \
							  "datasets/" + resource['id'],
			"dataset_name": ds_info['name']
		}

		centroid = None
		bbox = None
		for entry in terra_md['spatial_metadata']:
			if 'centroid' in terra_md['spatial_metadata'][entry]:
				centroid = terra_md['spatial_metadata'][entry]['centroid']
			if 'bounding_box' in terra_md['spatial_metadata'][entry]:
				bbox = terra_md['spatial_metadata'][entry]['bounding_box']
				bbox = {
					"type": bbox['type'],
					"coordinates": [
						bbox['coordinates']
					]
				}

		if 'site_metadata' in terra_md:
			# We've already determined the plot associated with this dataset so we can skip some work
			self.log_info(resource, "Creating datapoint without lookup in %s" % streamprefix)
			create_datapoint_with_dependencies(connector, host, secret_key,
											   streamprefix, centroid,
											   scan_time, scan_time, dpmetadata, date, bbox,
											   terra_md['site_metadata']['sitename'])

		else:
			# We need to do the traditional querying for plot
			self.log_info(resource, "Creating datapoint with lookup in %s" % streamprefix)
			create_datapoint_with_dependencies(connector, host, secret_key,
											   streamprefix, centroid,
											   scan_time, scan_time, dpmetadata, date, bbox)

		# Attach geometry to Clowder metadata as well
		self.log_info(resource, "Uploading dataset metadata")
		ext_meta = build_metadata(host, self.extractor_info, resource['id'], {
			"datapoints_added": 1
		}, 'dataset')
		upload_metadata(connector, host, secret_key, resource['id'], ext_meta)

		self.end_message(resource)

# @end extractor_sensor_position

if __name__ == "__main__":
	extractor = Sensorposition2Geostreams()
	extractor.start()
