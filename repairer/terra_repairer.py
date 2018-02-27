#!/usr/bin/env python

import os
import requests
import logging

from pyclowder.utils import CheckMessage
from pyclowder.datasets import upload_metadata, download_metadata, submit_extraction
from pyclowder.files import download_info
from terrautils.extractors import TerrarefExtractor, delete_dataset_metadata, load_json_file, upload_to_dataset


def add_local_arguments(parser):
	# add any additional arguments to parser
	parser.add_argument('--callback', default=os.getenv('CALLBACK_EXTRACTOR', ""),
						help="user ID to use as creator of metadata")

class RepairLemnatecDatasets(TerrarefExtractor):
	def __init__(self):
		super(RepairLemnatecDatasets, self).__init__()

		add_local_arguments(self.parser)

		# parse command line and load default logging configuration
		self.setup(sensor='sensorposition')

		# assign local arguments
		self.callback = self.args.callback

	# Check whether dataset has geospatial metadata
	def check_message(self, connector, host, secret_key, resource, parameters):
		return CheckMessage.bypass

	# Process the file and upload the results
	def process_message(self, connector, host, secret_key, resource, parameters):
		self.start_message()

		sensor_type, timestamp = resource['name'].split(" - ")
		targets = self.get_targets_by_sensor(sensor_type)
		source = self.get_source_by_sensor(sensor_type)
		existing_files = {}
		for t in targets:
			for f in resource['files']:
				if f['filename'].endswith(t):
					logging.getLogger(__name__).info("Found %s" % f['filename'])
					existing_files[t] = f['filename']
					break

		if len(existing_files) == len(targets):
			logging.getLogger(__name__).info("Target files already exist")

			# If there are bin2tif files previously created, are they valid?
			dsmd = download_metadata(connector, host, secret_key, resource['id'])
			for md in dsmd:
				if 'extractor_id' in md['agent'] and md['agent']['extractor_id'].endswith(source):
					# Found bin2tif metadata - are previously created files valid?
					logging.getLogger(__name__).info("Found metadata from %s" % source)
					for url in md['content']['files_created']:
						fid = url.split("/")[-1]
						i = download_info(connector, host, secret_key, fid)
						i = self.remapMountPath(connector, i['filepath'])
						logging.getLogger(__name__).info("Checking validity of %s" % i)
						if not os.path.isfile(i):
							# Found invalid file - nuke the entire site from orbit
							logging.getLogger(__name__).info("Invalid; deleting metadata")
							self.delete_dataset_metadata(host, self.clowder_user, self.clowder_pass, resource['id'], source)

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

							break

		else:
			# Search for target source files
			source_dir = os.path.dirname(self.sensors.get_sensor_path_by_dataset(resource['name']))
			source_dir = self.remapMountPath(connector, source_dir)
			if sensor_type == "scanner3DTop":
				source_dir = source_dir.replace("Level_1", "raw_data")

			logging.getLogger(__name__).info("Searching for target files in %s" % source_dir)

			if os.path.isdir(source_dir):
				targ_files = {}
				for f in os.listdir(source_dir):
					for t in targets:
						if f.endswith(t):
							targ_files[t] = os.path.join(source_dir, f)
							break

				if targ_files != {}:
					for t in targ_files:
						logging.getLogger(__name__).info("Uploading %s" % (targ_files[t]))
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
					logging.getLogger(__name__).error("targets not found in %s" % source_dir)

			else:
				logging.getLogger(__name__).info("%s could not be found" % source_dir)

		#self.end_message()

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

	def get_source_by_sensor(self, sensor_type):
		callbacks = {
			"stereoTop": "terra.stereo-rgb.bin2tif",

			"flirIrCamera": "terra.multispectral.flir2tif",

			"scanner3DTop": ""
		}

		if sensor_type in callbacks:
			return callbacks[sensor_type]
		else:
			return None

	def delete_dataset_metadata(self, host, clowder_user, clowder_pass, datasetid, ext):
		url = "%sapi/datasets/%s/metadata.jsonld?extractor=%s" % (host, datasetid, ext)

		result = requests.delete(url, stream=True, auth=(clowder_user, clowder_pass))
		result.raise_for_status()

		return json.loads(result.text)

if __name__ == "__main__":
	extractor = RepairLemnatecDatasets()
	extractor.start()
