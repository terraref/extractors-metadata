#!/usr/bin/env python

import os
import logging

from pyclowder.utils import CheckMessage
from pyclowder.datasets import upload_metadata, submit_extraction,
from terrautils.extractors import TerrarefExtractor, delete_dataset_metadata, load_json_file
from terrautils.extractors import upload_to_dataset


def add_local_arguments(parser):
	# add any additional arguments to parser
	parser.add_argument('--userid', default=os.getenv('CLOWDER_USER_UUID', "57adcb81c0a7465986583df1"),
						help="user ID to use as creator of metadata")
	parser.add_argument('--callback', default=os.getenv('CALLBACK_EXTRACTOR', ""),
						help="user ID to use as creator of metadata")

class RepairLemnatecDatasets(TerrarefExtractor):
	def __init__(self):
		super(RepairLemnatecDatasets, self).__init__()

		add_local_arguments(self.parser)

		# parse command line and load default logging configuration
		self.setup(sensor='sensorposition')

		# assign local arguments
		self.delete = self.args.delete
		self.userid = self.args.userid
		self.callback = self.args.callback

	# Check whether dataset has geospatial metadata
	def check_message(self, connector, host, secret_key, resource, parameters):
		return CheckMessage.bypass

	# Process the file and upload the results
	def process_message(self, connector, host, secret_key, resource, parameters):
		self.start_message()

		sensor_type, timestamp = resource['name'].split(" - ")

		# Search for metadata.json source file
		source_dir = os.path.dirname(self.sensors.get_sensor_path_by_dataset(resource['name']))
		source_dir = self.remapMountPath(connector, source_dir)

		# TODO: Eventually we should find better way to represent this
		# TODO: split between the PLY files (in Level_1) and metadata.json files
		if sensor_type == "scanner3DTop":
			source_dir = source_dir.replace("Level_1", "raw_data")

		logging.getLogger(__name__).info("Searching for target files in %s" % source_dir)

		if os.path.isdir(source_dir):
			targets = self.get_targets_by_sensor(sensor_type)
			targ_files = {}
			for f in os.listdir(source_dir):
				for t in targets:
					if f.endswith(t):
						targ_files[t] = os.path.join(source_dir, f)

			if targ_files != {}:
				for t in targ_files:
					logging.getLogger(__name__).info("Uploading cleaned metadata to %s" % resource['id'])
					upload_to_dataset(connector, host, self.clowder_user, self.clowder_pass, resource['id'], targ_files[t])

				# Now trigger a callback extraction if given
				if len(self.callback) > 0:
					logging.getLogger(__name__).info("Submitting callback extraction to %s" % self.callback)
					submit_extraction(connector, host, secret_key, resource['id'], self.callback)
				else:
					callbacks = self.get_callbacks_by_sensor(sensor_type)
					if callbacks:
						for c in callbacks:
							logging.getLogger(__name__).info("Submitting callback extraction to %s" % c)
							submit_extraction(connector, host, secret_key, resource['id'], c)
					else:
						logging.getLogger(__name__).info("No default callback found for %s" % sensor_type)
			else:
				logging.getLogger(__name__).error("metadata.json not found in %s" % source_dir)

		else:
			logging.getLogger(__name__).info("%s could not be found" % source_dir)

		# TODO: Have extractor check for existence of Level_1 output product and delete if exists?

		self.end_message()

	def remapMountPath(self, connector, path):
		if len(connector.mounted_paths) > 0:
			for source_path in connector.mounted_paths:
				if path.startswith(source_path):
					return path.replace(source_path, connector.mounted_paths[source_path])
			return path
		else:
			return path

	def get_callbacks_by_sensor(self, sensor_type):
		"""Return list of standard extractors to trigger based on input sensor."""
		callbacks = {
			"stereoTop": ["terra.stereo-rgb.bin2tif",
						  "terra.metadata.sensorposition"],

			"flirIrCamera": ["terra.multispectral.flir2tif",
							 "terra.metadata.sensorposition"],

			"scanner3DTop": ["terra.3dscanner.ply2las",
							 "terra.3dscanner.heightmap",
							 "terra.metadata.sensorposition"]
		}

		if sensor_type in callbacks:
			return callbacks[sensor_type]
		else:
			return None

	def get_targets_by_sensor(self, sensor_type):
		"""Return list of file extensions to find based on input sensor."""
		callbacks = {
			"stereoTop": ["_left.bin",
						  "_right.bin"],

			"flirIrCamera": ["_ir.bin"],

			"scanner3DTop": []
		}

		if sensor_type in callbacks:
			return callbacks[sensor_type]
		else:
			return None

if __name__ == "__main__":
	extractor = RepairLemnatecDatasets()
	extractor.start()
