from lib.classes import retrieveData

import json
import os, sys, json, time, uuid, datetime as dt
from google.oauth2 import service_account
from google.cloud import storage
from google.cloud import bigquery 
import subprocess
import argparse


SERVICE_ACC = '/home/hafizaimanhassan/airflow/image/api_bq/secret/subtle-bit-368908-6b0762c7789c.json'
#SERVICE_ACC = '/Users/hafizaimandinie/Documents/api-to-bq-master/schema/secret1/subtle-bit-368908-6b0762c7789c.json'
os.environ["PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION"]='python'   ## Issue with protobuf, must be <= 3.20.1
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=SERVICE_ACC
cred01 = service_account.Credentials.from_service_account_file(SERVICE_ACC)

pipeline = retrieveData(
    url = 'https://api.data.gov.sg/v1/transport/taxi-availability',
    method = "GET",
    project_id = "subtle-bit-368908",
    dataset_id = "sgtaxi",
    table_name = "history_ride_available",
    write_disposition = "WRITE_APPEND"
)

pipeline.make_request()
pipeline.transform()
pipeline.upload_to_bq()