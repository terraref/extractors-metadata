# =============================================================================
#
# In order for this extractor to run according to your preferences, 
# the following parameters need to be set. 
# 
# Some parameters can be left with the default values provided here - in that 
# case it is important to verify that the default value is appropriate to 
# your system. It is especially important to verify that paths to files and 
# software applications are valid in your system.
#
# =============================================================================

import os

# name to show in rabbitmq queue list
extractorName = os.getenv('RABBITMQ_QUEUE', "terra.sensorposition")

# URL to be used for connecting to rabbitmq
# rabbitmqURL = os.getenv('RABBITMQ_URI', "amqp://guest:guest@rabbitmq.ncsa.illinois.edu:5672/clowder-dev")
rabbitmqURL = os.getenv('RABBITMQ_URI', "amqp://guest:guest@localhost:5672/%2f")

# name of rabbitmq exchange
# rabbitmqExchange = os.getenv('RABBITMQ_EXCHANGE', "terra")
rabbitmqExchange = os.getenv('RABBITMQ_EXCHANGE', "clowder")

# type of files to process
messageType = "*.metadata.added"

# trust certificates, set this to false for self signed certificates
sslVerify = os.getenv('RABBITMQ_SSLVERIFY', False)

# Comma delimited list of endpoints and keys for registering extractor information
registrationEndpoints = os.getenv('REGISTRATION_ENDPOINTS', "")

# Dictionary that maps {"remote Clowder source path": "local mounted path"} for streamlining Clowder downloads
mountedPaths = {"/home/clowder/sites": "/home/extractor/sites"}

# Stream in Geostreams API to upload datapoints into
geostream_id = "1"
