#!/usr/bin/env python

import logging
import utm
import os
import time
import json
import requests

from pyclowder.extractors import Extractor
from pyclowder.utils import CheckMessage
import pyclowder.datasets
import pyclowder.geostreams

import plotid_by_latlon


class Sensorposition2Geostreams(Extractor):
	def __init__(self):
		Extractor.__init__(self)

		self.parser.add_argument('--plots', dest="plots_shp", type=str, nargs='?',
								 default="/home/extractor/extractors-metadata/sensorposition/shp/sorghumexpfall2016v5/sorghumexpfall2016v5_lblentry_1to7.shp",
								 help=".shp file containing plots")

		# parse command line and load default logging configuration
		self.setup()

		# setup logging for the exctractor
		logging.getLogger('pyclowder').setLevel(logging.DEBUG)
		logging.getLogger('__main__').setLevel(logging.DEBUG)

		self.plots_shp = self.args.plots_shp

	# Check whether dataset has geospatial metadata
	def check_message(self, connector, host, secret_key, resource, parameters):
		if 'metadata' in resource:
			gantry_x, gantry_y, loc_cambox_x, loc_cambox_y, fov_x, fov_y, ctime = fetch_md_parts(resource['metadata'])
			if gantry_x and gantry_y and loc_cambox_x and loc_cambox_y and fov_x and fov_y:
				return CheckMessage.bypass

		# If we didn't find required metadata info, don't process this dataset
		logging.info("...did not find required geopositional metadata; skipping %s" % resource['id'])
		return CheckMessage.ignore

	# Process the file and upload the results
	def process_message(self, connector, host, secret_key, resource, parameters):
		# Pull positional information from metadata
		gantry_x, gantry_y, loc_cambox_x, loc_cambox_y, fov_x, fov_y, ctime = fetch_md_parts(resource['metadata'])

		# Convert positional information into FOV polygon -----------------------------------------------------
		# GANTRY GEOM (LAT-LONG) ##############
		# NW: 33d 04.592m N , -111d 58.505m W #
		# NE: 33d 04.591m N , -111d 58.487m W #
		# SW: 33d 04.474m N , -111d 58.505m W #
		# SE: 33d 04.470m N , -111d 58.485m W #
		#######################################
		SE_latlon = (33.0745, -111.97475)
		SE_utm = utm.from_latlon(SE_latlon[0], SE_latlon[1])

		# GANTRY GEOM (GANTRY CRS) ############
		#			      N(x)                #
		#			      ^                   #
		#			      |                   #
		#			      |                   #
		#			      |                   #
		#      W(y)<------SE                  #
		#                                     #
		# NW: (207.3,	22.135,	5.5)          #
		# SE: (3.8,	0.0,	0.0)              #
		#######################################
		SE_offset_x = 3.8
		SE_offset_y = 0

		# Determine sensor position relative to origin and get lat/lon
		gantry_utm_x = SE_utm[0] - (gantry_y - SE_offset_y)
		gantry_utm_y = SE_utm[1] + (gantry_x - SE_offset_x)
		sensor_utm_x = gantry_utm_x - loc_cambox_y
		sensor_utm_y = gantry_utm_y + loc_cambox_x
		sensor_latlon = utm.to_latlon(sensor_utm_x, sensor_utm_y, SE_utm[2], SE_utm[3])
		logging.info("sensor lat/lon: %s" % str(sensor_latlon))

		# Determine field of view (assumes F.O.V. X&Y are based on center of sensor)
		fov_NW_utm_x = sensor_utm_x - fov_y/2
		fov_NW_utm_y = sensor_utm_y + fov_x/2
		fov_SE_utm_x = sensor_utm_x + fov_y/2
		fov_SE_utm_y = sensor_utm_y - fov_x/2
		fov_nw_latlon = utm.to_latlon(fov_NW_utm_x, fov_NW_utm_y, SE_utm[2],SE_utm[3])
		fov_se_latlon = utm.to_latlon(fov_SE_utm_x, fov_SE_utm_y, SE_utm[2], SE_utm[3])
		logging.debug("F.O.V. NW lat/lon: %s" % str(fov_nw_latlon))
		logging.debug("F.O.V. SE lat/lon: %s" % str(fov_se_latlon))

		# Upload data into Geostreams API -----------------------------------------------------
		fileIdList = []
		if 'type' in resource and resource['type'] == 'dataset':
			filelist = pyclowder.datasets.get_file_list(self, host, secret_key, resource['id'])
			for f in filelist:
				fileIdList.append(f['id'])

		# SENSOR is the plot
		sensor_data = pyclowder.geostreams.get_sensors_by_circle(connector, host, secret_key, sensor_latlon[1], sensor_latlon[0], 0.01)

		if not sensor_data:
			plot_info = plotid_by_latlon.plotQuery(self.plots_shp, sensor_latlon[1], sensor_latlon[0])
			plot_name = "Range "+plot_info['plot'].replace("-", " Pass ")
			logging.info("...creating plot: "+str(plot_info))
			sensor_id = pyclowder.geostreams.create_sensor(connector, host, secret_key, plot_name,{
				"type": "Point",
				"coordinates": [plot_info['point'][1], plot_info['point'][0], plot_info['point'][2]]
			}, {
				"id": "MAC Field Scanner",
				"title": "MAC Field Scanner",
				"sensorType": 4
			}, "Maricopa")
		else:
			sensor_id = sensor_data['id']
			plot_name = sensor_data['name']

		# STREAM is plot x instrument
		if 'dataset_info' in resource:
			stream_name = resource['dataset_info']['name']
		elif 'type' in resource and resource['type'] == 'dataset':
			ds_info = pyclowder.datasets.get_info(connector, host, secret_key, resource['id'])
			stream_name = ds_info['name']
		if stream_name.find(' - ') > -1:
			stream_name = stream_name.split(' - ')[0]
		stream_name = stream_name + " - " + plot_name

		stream_data = pyclowder.geostreams.get_stream_by_name(connector, host, secret_key, stream_name)
		if not stream_data:
			stream_id = pyclowder.geostreams.create_stream(connector, host, secret_key, stream_name, sensor_id, {
				"type": "Point",
				"coordinates": [sensor_latlon[1], sensor_latlon[0], 0]
			})
		else: stream_id = stream_data['id']

		logging.info("posting datapoint to stream %s" % stream_id)
		metadata = {
			"sources": host+resource['type']+"s/"+resource['id'],
			"file_ids": ",".join(fileIdList),
			"centroid": {
				"type": "Point",
				"coordinates": [sensor_latlon[1], sensor_latlon[0]]
			},
			"fov": {
				"type": "Polygon",
				"coordinates": [[[fov_nw_latlon[1], fov_nw_latlon[0], 0],
								 [fov_nw_latlon[1], fov_se_latlon[0], 0],
								 [fov_se_latlon[1], fov_se_latlon[0], 0],
								 [fov_se_latlon[1], fov_nw_latlon[0], 0],
								 [fov_nw_latlon[1], fov_nw_latlon[0], 0] ]]
			}
		}

		# Format time properly, adding UTC if missing from Danforth timestamp
		time_obj = time.strptime(ctime, "%m/%d/%Y %H:%M:%S")
		time_fmt = time.strftime('%Y-%m-%dT%H:%M:%S', time_obj)
		if len(time_fmt) == 19:
			time_fmt += "-06:00"

		pyclowder.geostreams.create_datapoint(connector, host, secret_key, stream_id, {
			"type": "Point",
			"coordinates": [sensor_latlon[1], sensor_latlon[0], 0]
		}, time_fmt, time_fmt, metadata)

# Try several variations on each position field to get all required information
def fetch_md_parts(metadata):
	gantry_x, gantry_y = None, None
	loc_cambox_x, loc_cambox_y = None, None
	fov_x, fov_y = None, None
	ctime = None

	"""
		Due to observed differences in metadata field names over time, this method is
		flexible with respect to finding fields. By default each entry for each field
		is checked with both a lowercase and uppercase leading character.
	"""

	if 'lemnatec_measurement_metadata' in metadata:
		lem_md = metadata['lemnatec_measurement_metadata']
		if 'gantry_system_variable_metadata' in lem_md and 'sensor_fixed_metadata' in lem_md:
			gantry_meta = lem_md['gantry_system_variable_metadata']
			sensor_meta = lem_md['sensor_fixed_metadata']

			# X and Y position of gantry
			x_positions = ['position x [m]', 'position X [m]']
			for variant in x_positions:
				val = check_field_variants(gantry_meta, variant)
				if val:
					gantry_x = parse_as_float(val)
					break
			y_positions = ['position y [m]', 'position Y [m]']
			for variant in y_positions:
				val = check_field_variants(gantry_meta, variant)
				if val:
					gantry_y = parse_as_float(val)
					break

			# Sensor location within camera box
			cbx_locations = ['location in camera box x [m]', 'location in camera box X [m]']
			for variant in cbx_locations:
				val = check_field_variants(sensor_meta, variant)
				if val:
					loc_cambox_x = parse_as_float(val)
					break
			cby_locations = ['location in camera box y [m]', 'location in camera box Y [m]']
			for variant in cby_locations:
				val = check_field_variants(sensor_meta, variant)
				if val:
					loc_cambox_y = parse_as_float(val)
					break

			# Field of view
			x_fovs = ['field of view x [m]', 'field of view X [m]']
			for variant in x_fovs:
				val = check_field_variants(sensor_meta, variant)
				if val:
					fov_x = parse_as_float(val)
					break
			y_fovs = ['field of view y [m]', 'field of view Y [m]']
			for variant in y_fovs:
				val = check_field_variants(sensor_meta, variant)
				if val:
					fov_y = parse_as_float(val)
					break
			if not (fov_x and fov_y):
				val = check_field_variants(sensor_meta, 'field of view at 2m in X- Y- direction [m]')
				if val:
					vals = val.replace('[','').replace(']','').split(' ')
					if not fov_x:
						fov_x = parse_as_float(vals[0])
					if not fov_y:
						fov_y = parse_as_float(vals[1])

			# timestamp, e.g. "2016-05-15T00:30:00-05:00"
			val = check_field_variants(gantry_meta, 'time')
			if val:
				ctime = val.encode("utf-8")
			else:
				ctime = "unknown"

	return gantry_x, gantry_y, loc_cambox_x, loc_cambox_y, fov_x, fov_y, ctime

# Check for fieldname in dict, including capitalization changes
def check_field_variants(dict, key):
	if key in dict:
		return dict[key]
	elif key.capitalize() in dict:
		return dict[key.capitalize()]
	else:
		return False

# Try to convert val to float, return val on Exception
def parse_as_float(val):
	try:
		return float(val.encode("utf-8"))
	except AttributeError:
		return val

if __name__ == "__main__":
	extractor = Sensorposition2Geostreams()
	extractor.start()
