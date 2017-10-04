#!/usr/bin/env python

import os
import logging

from pyclowder.utils import CheckMessage
from pyclowder.datasets import upload_metadata
from terrautils.extractors import TerrarefExtractor, delete_dataset_metadata, load_json_file
from terrautils.metadata import clean_metadata


def add_local_arguments(parser):
	# add any additional arguments to parser
	parser.add_argument('--delete', type=bool, default=os.getenv('DELETE_EXISTING_METADATA', True),
						help="whether to delete all existing metadata from datasets first")
	parser.add_argument('--userid', default=os.getenv('CLOWDER_USER_UUID', "57adcb81c0a7465986583df1"),
						help="user ID to use as creator of metadata")

class ReCleanLemnatecMetadata(TerrarefExtractor):
	def __init__(self):
		super(ReCleanLemnatecMetadata, self).__init__()

		add_local_arguments(self.parser)

		# parse command line and load default logging configuration
		self.setup(sensor='sensorposition')

		# assign local arguments
		self.delete = self.args.delete
		self.userid = self.args.userid

	# Check whether dataset has geospatial metadata
	def check_message(self, connector, host, secret_key, resource, parameters):
		return CheckMessage.bypass

	# Process the file and upload the results
	def process_message(self, connector, host, secret_key, resource, parameters):
		self.start_message()

		sensor_type, timestamp = resource['name'].split(" - ")

		if self.delete:
			# Delete all existing metadata from this dataset
			delete_dataset_metadata(host, self.clowder_user, self.clowder_pass, resource['id'])

		# Search for metadata.json source file
		source_dir = os.path.dirname(self.sensors.get_sensor_path_by_dataset(resource['name']))
		source_dir = self.remapMountPath(connector, source_dir)

		if os.path.isdir(source_dir):
			md_file = None
			for f in os.listdir(source_dir):
				if f.endswith("metadata.json"):
					md_file = os.path.join(source_dir, f)

			if md_file:
				md_json = clean_metadata(load_json_file(md_file), sensor_type)
				format_md = {
					"@context": ["https://clowder.ncsa.illinois.edu/contexts/metadata.jsonld",
								 {"@vocab": "https://terraref.ncsa.illinois.edu/metadata/uamac#"}],
					"content": md_json,
					"agent": {
						"@type": "cat:user",
						"user_id": "https://terraref.ncsa.illinois.edu/clowder/api/users/%s" % self.userid
					}
				}
				upload_metadata(connector, host, secret_key, resource['id'], format_md)
			else:
				logging.error("metadata.json not found in %s" % source_dir)

		else:
			logging.info("%s could not be found" % source_dir)

		self.end_message()

	def remapMountPath(self, connector, path):
		if len(connector.mounted_paths) > 0:
			for source_path in connector.mounted_paths:
				if path.startswith(source_path):
					return path.replace(source_path, connector.mounted_paths[source_path])
			return path
		else:
			return path


if __name__ == "__main__":
	extractor = ReCleanLemnatecMetadata()
	extractor.start()
