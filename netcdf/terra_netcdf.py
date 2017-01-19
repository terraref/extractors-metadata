#!/usr/bin/env python
import logging
import os
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

	# Check whether dataset already has metadata
	def check_message(self, connector, host, secret_key, resource, parameters):
		# For now if the dataset already has metadata from this extractor, don't recreate
		md = pyclowder.datasets.download_metadata(connector, host, secret_key,
												  parameters['datasetId'], self.extractor_info['name'])
		if len(md) > 0:
			for m in md:
				if 'agent' in m and 'name' in m['agent']:
					if m['agent']['name'].find(self.extractor_info['name']) > -1:
						print("skipping file %s, already processed" % resource['id'])
						return CheckMessage.ignore

		return CheckMessage.download

	# Process the file and upload the results
	def process_message(self, connector, host, secret_key, resource, parameters):
		ds_md = pyclowder.datasets.get_info(connector, host, secret_key, resource['parent']['id'])

		# Determine output file path
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
		if self.output_dir != '':
			outPath = os.path.join(self.output_dir, subPath, resource['name'].replace(".nc", ""))
		else:
			outPath = resource['name'].replace(".nc", "")

		logging.info('...extracting metadata in cdl format')
		metaFilePath = outPath + '.cdl'
		with open(metaFilePath, 'w') as fmeta:
			subprocess.call(['ncks', '--cdl', '-m', '-M', resource['local_paths'][0]], stdout=fmeta)
		if os.path.exists(metaFilePath):
			pyclowder.files.upload_to_dataset(connector, host, secret_key, resource['parent']['id'], metaFilePath)

		logging.info('...extracting metadata in xml format')
		metaFilePath = outPath + '.xml'
		with open(metaFilePath, 'w') as fmeta:
			subprocess.call(['ncks', '--xml', '-m', '-M', resource['local_paths'][0]], stdout=fmeta)
		if os.path.exists(metaFilePath):
			pyclowder.files.upload_to_dataset(connector, host, secret_key, resource['parent']['id'], metaFilePath)

		logging.info('...extracting metadata in json format')
		metaFilePath = outPath + '.json'
		with open(metaFilePath, 'w') as fmeta:
			subprocess.call(['ncks', '--jsn', '-m', '-M', resource['local_paths'][0]], stdout=fmeta)
		if os.path.exists(metaFilePath):
			pyclowder.files.upload_to_dataset(connector, host, secret_key, resource['parent']['id'], metaFilePath)

if __name__ == "__main__":
	extractor = NetCDFMetadataConversion()
	extractor.start()
