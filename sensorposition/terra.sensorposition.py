#!/usr/bin/env python
import logging
from config import *
import pyclowder.extractors as extractors
import utm
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
	#print(parameters)
	if 'metadata' in parameters:
		# Check properties of metadata for required geospatial information
		if 'lemnatec_measurement_metadata' in parameters['metadata']:
			lem_md = parameters['metadata']['lemnatec_measurement_metadata']
			if 'gantry_system_variable_metadata' in lem_md and 'sensor_fixed_metadata' in lem_md:
				gantryInfo = lem_md['gantry_system_variable_metadata']
				sensorInfo = lem_md['sensor_fixed_metadata']

				if (	'position x [m]' in gantryInfo and
						'position y [m]' in gantryInfo and
						'location in camera box x [m]' in sensorInfo and
						'location in camera box y [m]' in sensorInfo and
						'field of view x [m]' in sensorInfo and
						'field of view y [m]' in sensorInfo):
					return True

	# If we didn't find required metadata info, don't process this dataset
	return False

# Process the file and upload the results
def process_metadata(parameters):
	global geostream_id

	# Pull positional information from metadata -----------------------------------------------------

	gantry_meta = parameters['metadata']['lemnatec_measurement_metadata']['gantry_system_variable_metadata']
	sensor_meta = parameters['metadata']['lemnatec_measurement_metadata']['sensor_fixed_metadata']
	gantry_x = jsonToFloat(gantry_meta['position x [m]'])
	gantry_y = jsonToFloat(gantry_meta['position y [m]'])
	sensor_x = jsonToFloat(sensor_meta['location in camera box x [m]'])
	sensor_y = jsonToFloat(sensor_meta['location in camera box y [m]'])
	fov_x = jsonToFloat(sensor_meta['field of view x [m]'])
	fov_y = jsonToFloat(sensor_meta['field of view y [m]'])
	# timestamp, e.g. "2016-05-15T00:30:00-05:00"
	if 'Time' in gantry_meta:
		time = gantry_meta['Time'].encode("utf-8")
	else:
		time = "unknown"


	# Convert positional information into FOV polygon -----------------------------------------------------

	# GANTRY GEOM (LAT-LONG) ##############
	#	                                  #
	# NW: 33d 04.592m N , -111d 58.505m W #
	# NE: 33d 04.591m N , -111d 58.487m W #
	# SW: 33d 04.474m N , -111d 58.505m W #
	# SE: 33d 04.470m N , -111d 58.485m W #
	#	                                  #
	#######################################
	SE_latlon = (33.0745, -111.97475)
	SE_utm = utm.from_latlon(SE_latlon[0], SE_latlon[1])

	# GANTRY GEOM (GANTRY CRS) ############
	#                                     #
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
	sensor_utm_x = gantry_utm_x - sensor_y
	sensor_utm_y = gantry_utm_y + sensor_x
	sensor_latlon = utm.to_latlon(sensor_utm_x, sensor_utm_y, SE_utm[2], SE_utm[3])
	print("sensor lat/lon: %s" % sensor_latlon)

	# Determine field of view (assumes F.O.V. X&Y are based on center of sensor)
	fov_NW_utm_x = sensor_utm_x - fov_y/2
	fov_NW_utm_y = sensor_utm_y + fov_x/2
	fov_SE_utm_x = sensor_utm_x + fov_y/2
	fov_SE_utm_y = sensor_utm_y - fov_x/2
	fov_nw_latlon = utm.to_latlon(fov_NW_utm_x, fov_NW_utm_y, SE_utm[2],SE_utm[3])
	fov_se_latlon = utm.to_latlon(fov_SE_utm_x, fov_SE_utm_y, SE_utm[2], SE_utm[3])
	print("F.O.V. NW lat/lon: %s" % fov_nw_latlon)
	print("F.O.V. SE lat/lon: %s" % fov_se_latlon)


	# Upload data into Geostreams API -----------------------------------------------------
	fileIdList = []
	for f in parameters['filelist']:
		fileIdList.append(f['id'])

	# Metadata for datapoint properties
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
	# Actual data to be sent to Geostreams
	body = {"start_time": time,
			"end_time": time,
			"type": "Point",
			# TODO: Make this send the FOV polygon once Clowder supports it
			"geometry": {
				"type": "Point",
				"coordinates": [sensor_latlon[1], sensor_latlon[0], 0]
			},
			"properties": metadata,
			"stream_id": geostream_id
	}

	host = "https://terraref.ncsa.illinois.edu/clowder-dev/" # TODO: parameters['host']
	key = parameters['secretKey']
	r = requests.post(os.path.join(host,'api/geostreams/datapoints?key=%s' % key),
					  	data=json.dumps(body),
					  	headers={'Content-type': 'application/json'})

	if r.status_code != 200:
		print("ERROR: Could not add datapoint to stream : [%s] - %s" %  (str(r.status_code), r.text) )
	else:
		print "Successfully added datapoint."
	return

# Try to convert val to float, return val on Exception
def jsonToFloat(val):
	try:
		return float(val.encode("utf-8"))
	except AttributeError:
		return val


if __name__ == "__main__":
	main()
