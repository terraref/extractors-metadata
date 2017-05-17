#!/usr/bin/env python

import logging
import time
import datetime

from pyclowder.extractors import Extractor
from pyclowder.utils import CheckMessage
import pyclowder.datasets
import pyclowder.geostreams
import terrautils.extractors

import plotid_by_latlon


class Sensorposition2Geostreams(Extractor):
	def __init__(self):
		Extractor.__init__(self)

		self.parser.add_argument('--plots', dest="plots_shp", type=str, nargs='?',
								 default="/home/extractor/extractors-metadata/sensorposition/shp/sorghumexpfall2016v5/sorghumexpfall2016v5_lblentry_1to7.shp",
								 help=".shp file containing plots")
		self.parser.add_argument('--influxHost', dest="influx_host", type=str, nargs='?',
								 default="terra-logging.ncsa.illinois.edu", help="InfluxDB URL for logging")
		self.parser.add_argument('--influxPort', dest="influx_port", type=int, nargs='?',
								 default=8086, help="InfluxDB port")
		self.parser.add_argument('--influxUser', dest="influx_user", type=str, nargs='?',
								 default="terra", help="InfluxDB username")
		self.parser.add_argument('--influxPass', dest="influx_pass", type=str, nargs='?',
								 default="", help="InfluxDB password")
		self.parser.add_argument('--influxDB', dest="influx_db", type=str, nargs='?',
								 default="extractor_db", help="InfluxDB databast")

		# parse command line and load default logging configuration
		self.setup()

		# setup logging for the exctractor
		logging.getLogger('pyclowder').setLevel(logging.DEBUG)
		logging.getLogger('__main__').setLevel(logging.DEBUG)

		self.plots_shp = self.args.plots_shp
		self.influx_host = self.args.influx_host
		self.influx_port = self.args.influx_port
		self.influx_user = self.args.influx_user
		self.influx_pass = self.args.influx_pass
		self.influx_db = self.args.influx_db

	# Check whether dataset has geospatial metadata
	def check_message(self, connector, host, secret_key, resource, parameters):
		# Individual files do not have relevant metadata, only datasets
		if resource['type'] == 'dataset':
			(gantry_x, gantry_y, gantry_z, cambox_x, cambox_y, cambox_z,
			 fov_x, fov_y) = terrautils.extractors.geom_from_metadata(resource['metadata'])
			if (gantry_x and gantry_y and gantry_z and cambox_x and cambox_y and cambox_z and fov_x and fov_y):
				return CheckMessage.bypass
			else:
				logging.info("all geolocation metadata not found; skipping %s %s" % (resource['type'], resource['id']))

		return CheckMessage.ignore

	# Process the file and upload the results
	def process_message(self, connector, host, secret_key, resource, parameters):
		starttime = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
		created = 0
		bytes = 0

		# geoms = (bbox_se_lon, bbox_nw_lon, bbox_nw_lat, bbox_se_lat)
		ds_info = pyclowder.datasets.get_info(connector, host, secret_key, resource['parent']['id'])
		geoms = terrautils.extractors.calculate_gps_bounds(resource['metadata'], ds_info['name'])
		bbox = terrautils.extractors.calculate_bounding_box(geoms)
		centroid = [geoms[1] + ((geoms[0]-geoms[1])/2), geoms[3] + ((geoms[2]-geoms[3])/2)]

		# Upload data into Geostreams API -----------------------------------------------------
		fileIdList = []
		if 'type' in resource and resource['type'] == 'dataset':
			filelist = pyclowder.datasets.get_file_list(self, host, secret_key, resource['id'])
			for f in filelist:
				fileIdList.append(f['id'])

		# SENSOR is the plot
		# TODO: Replace with query to BETYdb
		sensor_data = pyclowder.geostreams.get_sensors_by_circle(connector, host, secret_key, centroid[1], centroid[0], 0.01)
		if not sensor_data:
			plot_info = plotid_by_latlon.plotQuery(self.plots_shp, centroid[1], centroid[0])
			plot_name = "Range "+plot_info['plot'].replace("-", " Pass ")
			logging.info("...creating plot: "+str(plot_info))
			sensor_id = pyclowder.geostreams.create_sensor(connector, host, secret_key, plot_name,{
				"type": "Point",
				"coordinates": [plot_info['point'][1], plot_info['point'][0], plot_info['point'][2]]
			}, {
				"id": "MAC Field Scanner",
				"title": "MAC Field Scanner",
				"sensorType": 4
			}, "Maricopa")
		else:
			sensor_id = sensor_data['id']
			plot_name = sensor_data['name']

		# STREAM is plot x instrument
		if 'dataset_info' in resource:
			stream_name = resource['dataset_info']['name']
		elif 'type' in resource and resource['type'] == 'dataset':
			ds_info = pyclowder.datasets.get_info(connector, host, secret_key, resource['id'])
			stream_name = ds_info['name']
		if stream_name.find(' - ') > -1:
			stream_name = stream_name.split(' - ')[0]
		stream_name = stream_name + " - " + plot_name

		stream_data = pyclowder.geostreams.get_stream_by_name(connector, host, secret_key, stream_name)
		if not stream_data:
			stream_id = pyclowder.geostreams.create_stream(connector, host, secret_key, stream_name, sensor_id, {
				"type": "Point",
				"coordinates": [centroid[1], centroid[0], 0]
			})
		else: stream_id = stream_data['id']

		logging.info("posting datapoint to stream %s" % stream_id)
		metadata = {
			"sources": host+resource['type']+"s/"+resource['id'],
			"file_ids": ",".join(fileIdList),
			"centroid": {
				"type": "Point",
				"coordinates": [centroid[1], centroid[0]]
			},
			"fov": {
				"type": "Polygon",
				"coordinates": [[bbox[0], bbox[1], bbox[2], bbox[3], bbox[0]]]
			}
		}

		# Format time properly
		scan_time = terrautils.extractors.calculate_scan_time(resource['metadata'])
		time_obj = time.strptime(scan_time, "%m/%d/%Y %H:%M:%S")
		time_fmt = time.strftime('%Y-%m-%dT%H:%M:%S', time_obj)
		if len(time_fmt) == 19:
			time_fmt += "-06:00"

		pyclowder.geostreams.create_datapoint(connector, host, secret_key, stream_id, {
			"type": "Point",
			"coordinates": [centroid[1], centroid[0], 0]
		}, time_fmt, time_fmt, metadata)

		# Attach geometry to Clowder metadata as well
		pyclowder.datasets.upload_metadata(connector, host, secret_key, resource['id'],
			terrautils.extractors.build_metadata(host, self.extractor_info['name'], resource['id'], metadata, 'dataset'))

		endtime = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
		terrautils.extractors.log_to_influxdb(self.extractor_info['name'], starttime, endtime, created, bytes)


if __name__ == "__main__":
	extractor = Sensorposition2Geostreams()
	extractor.start()
