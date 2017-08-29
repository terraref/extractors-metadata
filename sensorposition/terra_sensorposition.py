#!/usr/bin/env python

import logging
import time

from pyclowder.utils import CheckMessage
from pyclowder.datasets import get_info, get_file_list, upload_metadata
from terrautils.extractors import TerrarefExtractor, geom_from_metadata, build_metadata, \
	calculate_bounding_box, calculate_gps_bounds, calculate_scan_time, calculate_centroid
from terrautils.geostreams import create_datapoint_with_dependencies
from terrautils.betydb import get_sites_by_latlon
from terrautils.metadata import get_terraref_metadata

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
		if resource['type'] == 'metadata' and resource['parent']['type'] == 'dataset':
			if 'terraref_cleaned_metadata' in resource['metadata']:
				# Get dataset fixed metadata for geometry extraction
				ds_info = get_info(connector, host, secret_key, resource['parent']['id'])
				sensorname = ds_info['name'].split(' - ')[0]
				fullmd = get_terraref_metadata(resource['metadata'], sensorname)
				(gx, gy, gz, cx, cy, cz, fx, fy) = geom_from_metadata(fullmd)
				if (gx or gy or gz) and (cx or cy or cz) and (fx or fy):
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

		ds_info = get_info(connector, host, secret_key, resource['parent']['id'])
		geoms = calculate_gps_bounds(resource['metadata'], ds_info['name'])
		bbox = calculate_bounding_box(geoms)
		bbox_geom = {
			"type": "Polygon",
			"coordinates": [[bbox[0], bbox[1], bbox[2],
							 bbox[3], bbox[0]]]
		}
		centroid_latlon = calculate_centroid(geoms)

		# @begin convert_gantry_to_lat/lon
		# @in gantry_geometry
		# @out sensor_position_in_lat/lon
		# @end convert_gantry_to_lat/lon

		# Upload data into Geostreams API -----------------------------------------------------
		fileIdList = []
		if 'type' in resource['parent'] and resource['parent']['type'] == 'dataset':
			filelist = get_file_list(self, host, secret_key, resource['parent']['id'])
			for f in filelist:
				fileIdList.append(host+"files/"+f['id'])

		# @begin determine_plot_from_lat/lon
		# @in sensor_position_in_lat/lon
		# @out sensor_id
		# @end determine_plot_from_lat/lon

		# @begin upload_to_geostreams_API
		# @in sensor_position_in_lat/lon
		# @in sensor_id
		# @end upload_to_geostreams_API

		scan_time = calculate_scan_time(resource['metadata'])

		# Get sensor from datasetname
		(streamprefix, date) = ds_info['name'].split(' - ')
		date = date.split("__")[0]

		# Get plot names from BETYdb
		plots = []
		for site in get_sites_by_latlon(centroid_latlon, date):
			plots.append(site['sitename'])

		metadata = {
			"source_dataset": host+resource['parent']['type']+"s/"+resource['parent']['id'],
			"file_ids": ",".join(fileIdList),
			"plots": plots
		}
		create_datapoint_with_dependencies(connector, host, secret_key,
										   streamprefix, centroid_latlon,
										   scan_time, scan_time, metadata=metadata, filter_date=date, geom=bbox_geom)

		# Attach geometry to Clowder metadata as well
		metadata['plots'] = plots
		upload_metadata(connector, host, secret_key, resource['id'],
			build_metadata(host, self.extractor_info, resource['id'], metadata, 'dataset'))

		self.end_message()

# @end extractor_sensor_position

if __name__ == "__main__":
	extractor = Sensorposition2Geostreams()
	extractor.start()
