#!/usr/bin/env python

import datetime
import logging
import json
import os
import subprocess

from pyclowder.extractors import Extractor
from pyclowder.utils import CheckMessage
import pyclowder.files
import pyclowder.datasets
import terrautils.extractors


class NetCDFMetadataConversion(Extractor):
	def __init__(self):
		Extractor.__init__(self)

		influx_host = os.getenv("INFLUXDB_HOST", "terra-logging.ncsa.illinois.edu")
		influx_port = os.getenv("INFLUXDB_PORT", 8086)
		influx_db = os.getenv("INFLUXDB_DB", "extractor_db")
		influx_user = os.getenv("INFLUXDB_USER", "terra")
		influx_pass = os.getenv("INFLUXDB_PASSWORD", "")

		# add any additional arguments to parser
		self.parser.add_argument('--output', '-o', dest="output_dir", type=str, nargs='?',
								 default="/home/extractor/sites/ua-mac/Level_1/netcdf",
								 help="root directory where timestamp & output directories will be created")
		self.parser.add_argument('--overwrite', dest="force_overwrite", type=bool, nargs='?', default=False,
								 help="whether to overwrite output file if it already exists in output directory")
		self.parser.add_argument('--influxHost', dest="influx_host", type=str, nargs='?',
								 default=influx_host, help="InfluxDB URL for logging")
		self.parser.add_argument('--influxPort', dest="influx_port", type=int, nargs='?',
								 default=influx_port, help="InfluxDB port")
		self.parser.add_argument('--influxUser', dest="influx_user", type=str, nargs='?',
								 default=influx_user, help="InfluxDB username")
		self.parser.add_argument('--influxPass', dest="influx_pass", type=str, nargs='?',
								 default=influx_pass, help="InfluxDB password")
		self.parser.add_argument('--influxDB', dest="influx_db", type=str, nargs='?',
								 default=influx_db, help="InfluxDB database")

		# parse command line and load default logging configuration
		self.setup()

		# setup logging for the exctractor
		logging.getLogger('pyclowder').setLevel(logging.DEBUG)
		logging.getLogger('__main__').setLevel(logging.DEBUG)

		# assign other arguments
		self.output_dir = self.args.output_dir
		self.force_overwrite = self.args.force_overwrite
		self.influx_params = {
			"host": self.args.influx_host,
			"port": self.args.influx_port,
			"db": self.args.influx_db,
			"user": self.args.influx_user,
			"pass": self.args.influx_pass
		}

	# Check whether dataset already has output files
	def check_message(self, connector, host, secret_key, resource, parameters):
		ds_md = pyclowder.datasets.get_info(connector, host, secret_key, resource['parent']['id'])
		out_dir = terrautils.extractors.get_output_directory(self.output_dir, ds_md['name'], True)

		for out_type in ['.cdl', 'xml', '.json']:
			out_fname = terrautils.extractors.get_output_filename(ds_md['name'], out_type)
			if not os.path.isfile(os.path.join(out_dir, out_fname)):
				return CheckMessage.download

		logging.info("skipping %s; outputs already exist" % resource['id'])
		return CheckMessage.ignore

	# Process the file and upload the results
	def process_message(self, connector, host, secret_key, resource, parameters):
		starttime = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
		created = 0
		bytes = 0

		# Determine output file path
		ds_md = pyclowder.datasets.get_info(connector, host, secret_key, resource['parent']['id'])
		out_dir = terrautils.extractors.get_output_directory(self.output_dir, ds_md['name'], True)
		if not os.path.isdir(out_dir):
			os.makedirs(out_dir)
		out_fname_root = terrautils.extractors.get_output_filename(ds_md['name'], '')


		metaFilePath = os.path.join(out_dir, out_fname_root+'.cdl')
		if not os.path.isfile(metaFilePath) or self.force_overwrite:
			logging.info('...extracting metadata in cdl format: %s' % metaFilePath)
			with open(metaFilePath, 'w') as fmeta:
				subprocess.call(['ncks', '--cdl', '-m', '-M', resource['local_paths'][0]], stdout=fmeta)
			created += 1
			bytes += os.path.getsize(metaFilePath)
			pyclowder.files.upload_to_dataset(connector, host, secret_key, resource['parent']['id'], metaFilePath)

		metaFilePath = os.path.join(out_dir, out_fname_root+'.xml')
		if not os.path.isfile(metaFilePath) or self.force_overwrite:
			logging.info('...extracting metadata in xml format: %s' % metaFilePath)
			with open(metaFilePath, 'w') as fmeta:
				subprocess.call(['ncks', '--xml', '-m', '-M', resource['local_paths'][0]], stdout=fmeta)
			created += 1
			bytes += os.path.getsize(metaFilePath)
			pyclowder.files.upload_to_dataset(connector, host, secret_key, resource['parent']['id'], metaFilePath)

		metaFilePath = os.path.join(out_dir, out_fname_root+'.json')
		if not os.path.isfile(metaFilePath) or self.force_overwrite:
			logging.info('...extracting metadata in json format: %s' % metaFilePath)
			with open(metaFilePath, 'w') as fmeta:
				subprocess.call(['ncks', '--jsn', '-m', '-M', resource['local_paths'][0]], stdout=fmeta)
			created += 1
			bytes += os.path.getsize(metaFilePath)
			pyclowder.files.upload_to_dataset(connector, host, secret_key, resource['parent']['id'], metaFilePath)

			# Add json metadata to original netCDF file
			with open(metaFilePath, 'r') as metajson:
				metadata = terrautils.extractors.build_metadata(host, self.extractor_info['name'], resource['id'],
																json.load(metajson), 'dataset')
				pyclowder.files.upload_metadata(connector, host, secret_key, resource['parent']['id'], metadata)


		endtime = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
		terrautils.extractors.log_to_influxdb(self.extractor_info['name'], self.influx_params,
											  starttime, endtime, created, bytes)

if __name__ == "__main__":
	extractor = NetCDFMetadataConversion()
	extractor.start()
