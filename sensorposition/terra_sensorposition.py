#!/usr/bin/env python

import logging
import time

from pyclowder.utils import CheckMessage
from pyclowder.datasets import get_info, get_file_list, upload_metadata
from terrautils.extractors import TerrarefExtractor, geom_from_metadata, build_metadata, \
	calculate_bounding_box, calculate_gps_bounds, calculate_scan_time
from terrautils.geostreams import create_datapoint_with_dependencies


# @begin extractor_sensor_position
# @in new_dataset_added

class Sensorposition2Geostreams(TerrarefExtractor):
	def __init__(self):
		super(Sensorposition2Geostreams, self).__init__()

		# parse command line and load default logging configuration
		self.setup(sensor='sensorposition')

	# Check whether dataset has geospatial metadata
	def check_message(self, connector, host, secret_key, resource, parameters):
		# Individual files do not have relevant metadata, only datasets
		if resource['type'] == 'dataset':
			(gantry_x, gantry_y, gantry_z, cambox_x, cambox_y, cambox_z,
			 fov_x, fov_y) = geom_from_metadata(resource['metadata'])
			if (gantry_x and gantry_y and gantry_z and cambox_x and cambox_y and cambox_z and fov_x and fov_y):
				return CheckMessage.bypass
			else:
				logging.info("all geolocation metadata not found; skipping %s %s" % (resource['type'], resource['id']))

		return CheckMessage.ignore

	# Process the file and upload the results
	def process_message(self, connector, host, secret_key, resource, parameters):
		self.start_message()

		# @begin extract_positional_info_from_metadata
		# @in new_dataset_added
		# @out gantry_geometry
		# @end extract_positional_info

		# geoms = (bbox_se_lon, bbox_nw_lon, bbox_nw_lat, bbox_se_lat)
		ds_info = get_info(connector, host, secret_key, resource['parent']['id'])
		geoms = calculate_gps_bounds(resource['metadata'], ds_info['name'])
		bbox = calculate_bounding_box(geoms)
		centroid = [geoms[1] + ((geoms[0]-geoms[1])/2), geoms[3] + ((geoms[2]-geoms[3])/2)]

		# @begin convert_gantry_to_lat/lon
		# @in gantry_geometry
		# @out sensor_position_in_lat/lon
		# @end convert_gantry_to_lat/lon

		# Upload data into Geostreams API -----------------------------------------------------
		fileIdList = []
		if 'type' in resource and resource['type'] == 'dataset':
			filelist = get_file_list(self, host, secret_key, resource['id'])
			for f in filelist:
				fileIdList.append(f['id'])

		# @begin determine_plot_from_lat/lon
		# @in sensor_position_in_lat/lon
		# @out sensor_id
		# @end determine_plot_from_lat/lon

		# @begin upload_to_geostreams_API
		# @in sensor_position_in_lat/lon
		# @in sensor_id
		# @end upload_to_geostreams_API

		# Format time properly
		scan_time = calculate_scan_time(resource['metadata'])
		time_obj = time.strptime(scan_time, "%m/%d/%Y %H:%M:%S")
		time_fmt = time.strftime('%Y-%m-%dT%H:%M:%S', time_obj)
		if len(time_fmt) == 19:
			time_fmt += "-06:00"

		# Get sensor from datasetname
		if 'dataset_info' in resource:
			streamprefix = resource['dataset_info']['name']
		elif 'type' in resource and resource['type'] == 'dataset':
			ds_info = get_info(connector, host, secret_key, resource['id'])
			streamprefix = ds_info['name']
		if streamprefix.find(' - ') > -1:
			streamprefix = streamprefix.split(' - ')[0]

		metadata = {
			"sources": host+resource['type']+"s/"+resource['id'],
			"file_ids": ",".join(fileIdList),
			"centroid": {
				"type": "Point",
				"coordinates": [centroid[1], centroid[0]]
			},
			"fov": {
				"type": "Polygon",
				"coordinates": [[bbox[0], bbox[1], bbox[2], bbox[3], bbox[0]]]
			}
		}

		create_datapoint_with_dependencies(connector, host, secret_key,
										   streamprefix, [centroid[1], centroid[0]],
										   time_fmt, time_fmt, metadata=metadata)

		# Attach geometry to Clowder metadata as well
		upload_metadata(connector, host, secret_key, resource['id'],
			build_metadata(host, self.extractor_info['name'], resource['id'], metadata, 'dataset'))

		self.end_message()

# @end extractor_sensor_position

if __name__ == "__main__":
	extractor = Sensorposition2Geostreams()
	extractor.start()
