#!/usr/bin/env python
import logging
from config import *
import pyclowder.extractors as extractors
import utm
import time
import json
import requests



def main():
	global extractorName, messageType, rabbitmqExchange, rabbitmqURL, registrationEndpoints, mountedPaths

	#set logging
	logging.basicConfig(format='%(levelname)-7s : %(name)s -  %(message)s', level=logging.WARN)
	logging.getLogger('pyclowder.extractors').setLevel(logging.INFO)
	logger = logging.getLogger('extractor')
	logger.setLevel(logging.DEBUG)

	# setup
	extractors.setup(extractorName=extractorName,
					 messageType=messageType,
					 rabbitmqURL=rabbitmqURL,
					 rabbitmqExchange=rabbitmqExchange,
					 mountedPaths=mountedPaths)

	# register extractor info
	extractors.register_extractor(registrationEndpoints)

	#connect to rabbitmq
	extractors.connect_message_bus(extractorName=extractorName,
								   messageType=messageType,
								   processFileFunction=process_metadata,
								   checkMessageFunction=check_message,
								   rabbitmqExchange=rabbitmqExchange,
								   rabbitmqURL=rabbitmqURL)

# Check whether dataset has geospatial metadata
def check_message(parameters):
	if 'metadata' in parameters:
		gantry_x, gantry_y, loc_cambox_x, loc_cambox_y, fov_x, fov_y, ctime = fetch_md_parts(parameters['metadata'])
		if gantry_x and gantry_y and loc_cambox_x and loc_cambox_y and fov_x and fov_y:
			return True

	# If we didn't find required metadata info, don't process this dataset
	print("Did not find required geopositional metadata; skipping %s" % parameters['datasetId'])
	return False

# Process the file and upload the results
def process_metadata(parameters):
	global geostream_map

	host = parameters['host']
	key = parameters['secretKey']
	# Get sensor name from dataset name, e.g. "stereoTop 2016-01-01__12-12-12-123" = "stereoTop"
	sensor_name = parameters['datasetInfo']['name']
	if sensor_name.find(' - ') > -1:
		sensor_name = sensor_name.split(' - ')[0]

	# Pull positional information from metadata
	gantry_x, gantry_y, loc_cambox_x, loc_cambox_y, fov_x, fov_y, ctime = fetch_md_parts(parameters['metadata'])

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
	print("sensor lat/lon: %s" % str(sensor_latlon))

	# Determine field of view (assumes F.O.V. X&Y are based on center of sensor)
	fov_NW_utm_x = sensor_utm_x - fov_y/2
	fov_NW_utm_y = sensor_utm_y + fov_x/2
	fov_SE_utm_x = sensor_utm_x + fov_y/2
	fov_SE_utm_y = sensor_utm_y - fov_x/2
	fov_nw_latlon = utm.to_latlon(fov_NW_utm_x, fov_NW_utm_y, SE_utm[2],SE_utm[3])
	fov_se_latlon = utm.to_latlon(fov_SE_utm_x, fov_SE_utm_y, SE_utm[2], SE_utm[3])
	print("F.O.V. NW lat/lon: %s" % str(fov_nw_latlon))
	print("F.O.V. SE lat/lon: %s" % str(fov_se_latlon))

	# Upload data into Geostreams API -----------------------------------------------------
	fileIdList = []
	for f in parameters['filelist']:
		fileIdList.append(f['id'])

	# Metadata for datapoint properties
	if (sensor_name in geostream_map):
		stream_id = geostream_map[sensor_name]
	else:
		stream_id = get_stream_id(host, key, sensor_name)
		if not stream_id:
			stream_id = create_stream(host, key, sensor_name, {
				"type": "Point",
				"coordinates": sensor_latlon
			})

	print("posting datapoint to stream %s" % stream_id)
	metadata = {
		"sources": host+"datasets/"+parameters['datasetId'],
		"file_ids": ",".join(fileIdList),
		"centroid": {
			"type": "Point",
			"coordinates": [sensor_latlon[1], sensor_latlon[0]]
		},
		"fov": {
			"type": "Polygon",
			"coordinates": [[[fov_nw_latlon[1], fov_nw_latlon[0], 0],
							 [fov_se_latlon[1], fov_se_latlon[0], 0] ]]
		}
	}

	# Format time properly, adding UTC if missing from Danforth timestamp
	time_obj = time.strptime(ctime, "%m/%d/%Y %H:%M:%S")
	time_fmt = time.strftime('%Y-%m-%dT%H:%M:%S', time_obj)
	if len(time_fmt) == 19:
		time_fmt += "-06:00"

	# Actual data to be sent to Geostreams
	body = {"start_time": time_fmt,
			"end_time": time_fmt,
			"type": "Point",
			# TODO: Make this send the FOV polygon once Clowder supports it
			"geometry": {
				"type": "Point",
				"coordinates": [sensor_latlon[1], sensor_latlon[0], 0]
			},
			"properties": metadata,
			"stream_id": stream_id
	}

	# Make the POST
	r = requests.post(os.path.join(host,'api/geostreams/datapoints?key=%s' % key),
					  	data=json.dumps(body),
					  	headers={'Content-type': 'application/json'})

	if r.status_code != 200:
		print("ERROR: Could not add datapoint to stream : [%s]" %  r.status_code)
	else:
		print "Successfully added datapoint."
	return


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

# Get stream ID from Clowder based on stream name
def get_stream_id(host, key, name):
	if(not host.endswith("/")):
		host = host+"/"

	url = "%sapi/geostreams/streams?stream_name=%s&key=%s" % (host, name, key)
	print("...searching for stream ID: "+url)
	r = requests.get(url)
	if r.status_code == 200:
		json_data = r.json()
		for s in json_data:
			if 'name' in s and s['name'] == name:
				return s['id']
	else:
		print("error searching for stream ID")

	return None

def create_stream(host, key, name, geom):
	global sensor_id

	if(not host.endswith("/")):
		host = host+"/"

	body = {
		"name": name,
		"type": "point",
		"geometry": geom,
		"properties": {},
		"sensor_id": sensor_id
	}

	url = "%sapi/geostreams/streams?key=%s" % (host, key)
	print("...creating new stream: "+name)
	r = requests.post(url,
					  data=json.dumps(body),
					  headers={'Content-type': 'application/json'})
	if r.status_code == 200:
		return r.json()['id']
	else:
		print("error creating stream")

	return None

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
	main()
