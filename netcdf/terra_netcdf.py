#!/usr/bin/env python

import logging
import json
import os
import subprocess

from pyclowder.utils import CheckMessage
from pyclowder.files import upload_metadata, upload_to_dataset
from terrautils.extractors import TerrarefExtractor, build_metadata


class NetCDFMetadataConversion(TerrarefExtractor):
	def __init__(self):
		super(NetCDFMetadataConversion, self).__init__()

		# parse command line and load default logging configuration
		self.setup(sensor='netcdf_metadata')

	# Check whether dataset already has output files
	def check_message(self, connector, host, secret_key, resource, parameters):
		# TODO: can we check local path before processing .nc file?
		return CheckMessage.download

	# Process the file and upload the results
	def process_message(self, connector, host, secret_key, resource, parameters):
		self.start_message()

		# Put files alongside .nc file
		out_dir = os.path.dirname(resource['local_paths'][0])
		out_fname_root = resource['name'].replace('.nc', '')

		metaFilePath = os.path.join(out_dir, out_fname_root+'_metadata.cdl')
		if not os.path.isfile(metaFilePath) or self.force_overwrite:
			logging.info('...extracting metadata in cdl format: %s' % metaFilePath)
			with open(metaFilePath, 'w') as fmeta:
				subprocess.call(['ncks', '--cdl', '-m', '-M', resource['local_paths'][0]], stdout=fmeta)
			self.created += 1
			self.bytes += os.path.getsize(metaFilePath)
			upload_to_dataset(connector, host, secret_key, resource['parent']['id'], metaFilePath)

		metaFilePath = os.path.join(out_dir, out_fname_root+'._metadataxml')
		if not os.path.isfile(metaFilePath) or self.force_overwrite:
			logging.info('...extracting metadata in xml format: %s' % metaFilePath)
			with open(metaFilePath, 'w') as fmeta:
				subprocess.call(['ncks', '--xml', '-m', '-M', resource['local_paths'][0]], stdout=fmeta)
			self.created += 1
			self.bytes += os.path.getsize(metaFilePath)
			upload_to_dataset(connector, host, secret_key, resource['parent']['id'], metaFilePath)

		metaFilePath = os.path.join(out_dir, out_fname_root+'._metadata.json')
		if not os.path.isfile(metaFilePath) or self.force_overwrite:
			logging.info('...extracting metadata in json format: %s' % metaFilePath)
			with open(metaFilePath, 'w') as fmeta:
				subprocess.call(['ncks', '--jsn', '-m', '-M', resource['local_paths'][0]], stdout=fmeta)
			self.created += 1
			self.bytes += os.path.getsize(metaFilePath)
			upload_to_dataset(connector, host, secret_key, resource['parent']['id'], metaFilePath)

			# Add json metadata to original netCDF file
			with open(metaFilePath, 'r') as metajson:
				metadata = build_metadata(host, self.extractor_info['name'], resource['id'],
																json.load(metajson), 'dataset')
				upload_metadata(connector, host, secret_key, resource['parent']['id'], metadata)


		self.end_message()

if __name__ == "__main__":
	extractor = NetCDFMetadataConversion()
	extractor.start()
