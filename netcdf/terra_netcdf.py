#!/usr/bin/env python
import logging
import json
import os, shutil
import subprocess

from pyclowder.extractors import Extractor
from pyclowder.utils import CheckMessage
import pyclowder.files
import pyclowder.datasets

class NetCDFMetadataConversion(Extractor):
	def __init__(self):
		Extractor.__init__(self)

		# add any additional arguments to parser
		# self.parser.add_argument('--max', '-m', type=int, nargs='?', default=-1,
		#                          help='maximum number (default=-1)')
		self.parser.add_argument('--output', '-o', dest="output_dir", type=str, nargs='?',
								 default="/home/extractor/sites/ua-mac/Level_1/netcdf",
								 help="root directory where timestamp & output directories will be created")

		# parse command line and load default logging configuration
		self.setup()

		# setup logging for the exctractor
		logging.getLogger('pyclowder').setLevel(logging.DEBUG)
		logging.getLogger('__main__').setLevel(logging.DEBUG)

		# assign other arguments
		self.output_dir = self.args.output_dir

	# Check whether dataset already has output files
	def check_message(self, connector, host, secret_key, resource, parameters):
		ds_md = pyclowder.datasets.get_info(connector, host, secret_key, resource['parent']['id'])
		outPathRoot = self.getOutputFilename(ds_md['name'])

		foundAll = True
		for outpath in ['.cdl', 'xml', '.json']:
			if not os.path.isfile(os.path.join(outPathRoot, resource['name'].replace(".nc", outpath))):
				foundAll = False

		if not foundAll:
			return CheckMessage.download
		else:
			logger.info("skipping %s; outputs already exist" % resource['id'])
			return CheckMessage.ignore

	# Process the file and upload the results
	def process_message(self, connector, host, secret_key, resource, parameters):
		ds_md = pyclowder.datasets.get_info(connector, host, secret_key, resource['parent']['id'])

		# Determine output file path
		outPath = self.getOutputFilename(ds_md['name'])
		if not os.path.isdir(outPath):
			os.makedirs(outPath)

		inPath = resource['local_paths'][0]
		fileRoot = resource['name'].replace(".nc", "")

		metaFilePath = os.path.join(outPath, fileRoot+'.cdl')
		if not os.path.isfile(metaFilePath):
			logging.info('...extracting metadata in cdl format: %s' % metaFilePath)
			with open(metaFilePath, 'w') as fmeta:
				subprocess.call(['ncks', '--cdl', '-m', '-M', inPath], stdout=fmeta)
			if os.path.exists(metaFilePath):
				pyclowder.files.upload_to_dataset(connector, host, secret_key, resource['parent']['id'], metaFilePath)

		metaFilePath = os.path.join(outPath, fileRoot+'.xml')
		if not os.path.isfile(metaFilePath):
			logging.info('...extracting metadata in xml format: %s' % metaFilePath)
			with open(metaFilePath, 'w') as fmeta:
				subprocess.call(['ncks', '--xml', '-m', '-M', inPath], stdout=fmeta)
			if os.path.exists(metaFilePath):
				pyclowder.files.upload_to_dataset(connector, host, secret_key, resource['parent']['id'], metaFilePath)

		metaFilePath = os.path.join(outPath, fileRoot+'.json')
		if not os.path.isfile(metaFilePath):
			logging.info('...extracting metadata in json format: %s' % metaFilePath)
			with open(metaFilePath, 'w') as fmeta:
				subprocess.call(['ncks', '--jsn', '-m', '-M', inPath], stdout=fmeta)
			if os.path.exists(metaFilePath):
				pyclowder.files.upload_to_dataset(connector, host, secret_key, resource['parent']['id'], metaFilePath)
				with open(metaFilePath, 'r') as metajson:
					jdata = {
						# TODO: Generate JSON-LD context for additional fields
						"@context": ["https://clowder.ncsa.illinois.edu/contexts/metadata.jsonld"],
						"dataset_id": resource['parent']['id'],
						"content": json.load(metajson),
						"agent": {
							"@type": "cat:extractor",
							"extractor_id": host + "/api/extractors/" + self.extractor_info['name']
						}
					}
					pyclowder.datasets.upload_metadata(connector, host, secret_key, resource['parent']['id'], jdata)

	def getOutputFilename(self, ds_name):
		# Determine output file path
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

		return os.path.join(self.output_dir, subPath)

if __name__ == "__main__":
	extractor = NetCDFMetadataConversion()
	extractor.start()
