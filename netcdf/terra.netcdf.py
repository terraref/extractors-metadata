#!/usr/bin/env python
import logging
from config import *
import pyclowder.extractors as extractors
import subprocess

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
								   processFileFunction=process_file,
								   checkMessageFunction=check_message,
								   rabbitmqExchange=rabbitmqExchange,
								   rabbitmqURL=rabbitmqURL)

# Check whether dataset already has metadata
def check_message(parameters):
	# For now if the dataset already has metadata from this extractor, don't recreate
	md = extractors.download_dataset_metadata_jsonld(parameters['host'], parameters['secretKey'], parameters['datasetId'], extractorName)
	if len(md) > 0:
		for m in md:
			if 'agent' in m and 'name' in m['agent']:
				if m['agent']['name'].find(extractorName) > -1:
					print("skipping dataset %s, already processed" % parameters['datasetId'])
					return False

	return True

# Process the file and upload the results
def process_file(parameters):
	host = parameters['host']
	key = parameters['secretKey']
	ds_md = extractors.get_dataset_info(parameters['datasetId'], parameters)

	# Determine output file path
	outRootDir = "/home/extractor/sites/ua-mac/Level_1/netcdf"
	ds_name = ds_md['name']
	if ds_name.find(" - ") > -1:
		# sensor - timestamp
		ds_name_parts = ds_name.split(" - ")
		sensor_name = ds_name_parts[0]
		if ds_name_parts[1].find("__") > -1:
			# sensor - date__time
			ds_time_parts = ds_name_parts[1].split("__")
			timestamp = os.path.join(ds_time_parts[0], ds_name_parts[1])
		else:
			timestamp = ds_name_parts[1]
		# /sensor/date/time
		subPath = os.path.join(sensor_name, timestamp)
	else:
		subPath = ds_name
	outPath = os.path.join(outRootDir, subPath, parameters['filename'].replace(".nc", ""))

	print 'extracting metadata in cdl format'
	metaFilePath = outPath + '.cdl'
	with open(metaFilePath, 'w') as fmeta:
		subprocess.call(['ncks', '--cdl', '-m', '-M', parameters['inputfile']], stdout=fmeta)
	if os.path.exists(metaFilePath):
		extractors.upload_file_to_dataset(filepath=metaFilePath, parameters=parameters)

	print 'extracting metadata in xml format'
	metaFilePath = outPath + '.xml'
	with open(metaFilePath, 'w') as fmeta:
		subprocess.call(['ncks', '--xml', '-m', '-M', parameters['inputfile']], stdout=fmeta)
	if os.path.exists(metaFilePath):
		extractors.upload_file_to_dataset(filepath=metaFilePath, parameters=parameters)

if __name__ == "__main__":
	main()
