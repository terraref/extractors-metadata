#!/usr/bin/env python
import logging
from config import *
import pyclowder.extractors as extractors
import utm
import json
import requests



def main():
	global extractorName, messageType, rabbitmqExchange, rabbitmqURL    
	#set logging
	logging.basicConfig(format='%(levelname)-7s : %(name)s -  %(message)s', level=logging.WARN)
	logging.getLogger('pyclowder.extractors').setLevel(logging.INFO)
	#connect to rabbitmq
	extractors.connect_message_bus(extractorName=extractorName, messageType=messageType, 
		processFileFunction=process_file, rabbitmqExchange=rabbitmqExchange, rabbitmqURL=rabbitmqURL,
		checkMessageFunction=check_message)

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
def process_file(parameters):
	#
	data = parameters['metadata']
	# GET LOCATION INFO FROM METADATA ------------------------------------------------------------
	gantryInfo = data['lemnatec_measurement_metadata']['gantry_system_variable_metadata']
	sensorInfo = data['lemnatec_measurement_metadata']['sensor_fixed_metadata']

	#get position of gantry
	gantryX = jsonToFloat(gantryInfo['position x [m]'])
	gantryY = jsonToFloat(gantryInfo['position y [m]'])

	#get position of sensor
	sensorX = jsonToFloat(sensorInfo['location in camera box x [m]'])
	sensorY = jsonToFloat(sensorInfo['location in camera box y [m]'])

	#get field of view (assuming field of view X and field of view Y are based on the center of the sensor)
	fovX = jsonToFloat(sensorInfo['field of view x [m]'])
	fovY = jsonToFloat(sensorInfo['field of view y [m]'])

	#get timestamp
	#if 'Time' in gantryInfo:
	#	time = gantryInfo['Time'].encode("utf-8")
	#else:
	#	time = "unknown"
	time = "2016-05-15T00:30:00-05:00"

	# GET FIELD OF VIEW (FOV) FROM LOCATION ------------------------------------------------------------
	#############################################
	#SE Corner. 33d 04.470m N / -111d 58.485m W #
	#SW Corner. 33d 04.474m N / -111d 58.505m W #
	#NW Corner. 33d 04.592m N / -111d 58.505m W #
	#NE Corner. 33d 04.591m N / -111d 58.487m W #
	#############################################
	'''Yaping: What is the exact point on gantry that have the position recorded in the metadata?
	David: Not sure. I assume the SE corner of the sensor box and then the sensor position is relative to this
	rjstrand: With regard to the reported position, David is correct. The reported position represents the 
	 location of the SE corner of the camera box. Instrument positions are then based on offsets. Tino can speak to those.'''
	#the coordinate for position of gantry is with origin near SE (3.8, 0.0, 0.0),
	#positive x direction is to North and positive y direction is to West
	#			N(x)
	#			^
	#			|
	#			|
	#			|
	#W(y)<------SE
	#########################
	#SE (3.8,	0.0,	0.0)#
	#NW (207.3,	22.135,	5.5)#
	#########################
	SElon = -111.97475
	SElat = 33.0745
	#UTM coordinates
	SEutm = utm.from_latlon(SElat, SElon)
	#be careful
	gantryUTMx = SEutm[0] - gantryY
	gantryUTMy = SEutm[1] + (gantryX - 3.8)
	sensorUTMx = gantryUTMx - sensorY
	sensorUTMy = gantryUTMy + sensorX
	#get lat and lon of sensor
	sensorLatLon = utm.to_latlon(sensorUTMx, sensorUTMy, SEutm[2], SEutm[3])
	sensorLat = sensorLatLon[0]
	sensorLon = sensorLatLon[1]
	print("sensor lat/lon: (%s, %s)" % (sensorLat, sensorLon))

	#get NW and SE points of field of view bounding box
	#NW
	fovNWptUTMx = sensorUTMx - fovY/2
	fovNWptUTMy = sensorUTMy + fovX/2
	#SE
	fovSEptUTMx = sensorUTMx + fovY/2
	fovSEptUTMy = sensorUTMy - fovX/2

	fovNWptLatLon = utm.to_latlon(fovNWptUTMx,fovNWptUTMy,SEutm[2],SEutm[3])
	fovNWptLat = fovNWptLatLon[0]
	fovNWptLon = fovNWptLatLon[1]
	fovSEptLatLon = utm.to_latlon(fovSEptUTMx,fovSEptUTMy,SEutm[2],SEutm[3])
	fovSEptLat = fovSEptLatLon[0]
	fovSEptLon = fovSEptLatLon[1]
	print("FOV NW lat/lon: (%s, %s)" % (fovNWptLat, fovNWptLon))
	print("FOV SE lat/lon: (%s, %s)" % (fovSEptLat, fovSEptLon))

	# SAVE POSITION INTO GEOSTREAMS POSTGIS DB ------------------------------------------------------------
	#host = parameters['host']
	host = "https://terraref.ncsa.illinois.edu/clowder-dev/"

	key = parameters['secretKey']
	streamID = "1"
	fileIdList = []
	for f in parameters['filelist']:
		fileIdList.append(f['id'])

	# Properties of datapoint
	properties = {
		"sources": host+"datasets/"+parameters['datasetId'],
		"file_ids": ",".join(fileIdList),
		"fov": {
			"coordinates": [ [fovNWptLon, fovNWptLat, 0],
							 [fovSEptLon, fovSEptLat, 0] ]
		}
	}
	body = {"start_time": time,
			"end_time": time,
			"type": "Feature",
			"geometry": {
				"coordinates": [sensorLon, sensorLat, 0]
			},
			"properties": properties,
			"stream_id": streamID
	}

	r = requests.post(os.path.join(host,'api/geostreams/datapoints?key=%s' % key), data=json.dumps(body),
					  headers={'Content-type': 'application/json'})

	if r.status_code != 200:
		print("ERR  : Could not add datapoint to stream : [" + str(r.status_code) + "] - " + r.text)
	else:
		print "added datapoint!"
	return
	#################################################
	#################################################

def jsonToFloat(val):
	try:
		return float(val.encode("utf-8"))
	except AttributeError:
		return val

if __name__ == "__main__":
	main()
