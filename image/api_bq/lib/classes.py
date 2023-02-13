#from bson import json_util
import os, sys, json, time, uuid, datetime as dt
from google.oauth2 import service_account
from google.cloud import storage
from google.cloud import bigquery 
import subprocess
import argparse

import pandas as pd
import requests
import json
from pandas import json_normalize
from sklearn.cluster import KMeans
import numpy as np


class SourceError(Exception):
    """ An exception class for Source """
    pass

class retrieveData:

    def __init__(
        self,
        url: str='',
        method: str='GET',
        project_id: str=None,
        dataset_id: str=None,
        table_name: str=None,
        write_disposition: str=None,
    ):

        self.url = url
        self.method = method
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_name = table_name
        self.write_disposition = write_disposition


    def make_request(self):
        """ Make the HTTPS request """
        try:
            self.req = requests.get(self.url)
        except Exception as err:
            raise SourceError(err)

    def transform(self):
        """ Apply data transformation """

        taxi = json.loads(self.req.text)
        taxi_data = taxi['features'][0]['geometry']['coordinates']

        df = pd.DataFrame(taxi_data)
        df.columns = ['longitude1', 'latitude']
        df['longitude'] = df['longitude1']
        df = df.drop('longitude1', axis=1)
        df['coordinate'] = df['latitude'].astype(str) + ',' + df['longitude'].astype(str)
        df['timestamp'] = taxi['features'][0]['properties']['timestamp']

        """ Operation of KMeans Clustering for taxi Coordinates """

        kmeans = KMeans(n_clusters = 35, init ='k-means++')
        kmeans.fit(df[df.columns[0:2]]) # Compute k-means clustering.
        df['cluster_label'] = kmeans.fit_predict(df[df.columns[0:2]])

        """ Coordinates (city) reference"""

        df_loc = pd.read_csv('/home/hafizaimanhassan/airflow/image/api_bq/sg.csv')
        df_loc2 = df_loc[['lat', 'lng','city']].copy()
        df_loc2.columns = ['f_latitude', 'f_longitude', 'city']

        """ Nearest neighbor - to assign cluster """

        X = np.array(df[['latitude','longitude']])
        y = np.array(df['cluster_label'])
        from sklearn.neighbors import KNeighborsClassifier
        neigh = KNeighborsClassifier(n_neighbors=35)
        neigh.fit(X, y) 
        rest_x=np.array(df_loc2[['f_latitude','f_longitude']])
        rest_y=neigh.predict(rest_x)
        df3 = pd.DataFrame(rest_y, columns = ['cluster_label'])
        result = pd.concat([df_loc2, df3], axis=1, join='inner')
        result2 = result.groupby('cluster_label').first()

        #merge df between API data with reference coordinate
        dfinal = df.merge(result2, on="cluster_label", how = 'inner')
        dfinal['f_coordinate'] = dfinal['f_latitude'].astype(str) + ',' + dfinal['f_longitude'].astype(str) 

        #for Alert
        alertCity = dfinal.groupby(['city'])['city'].count().reset_index(name='ride_available')
        alertFN = alertCity[alertCity['ride_available'] < 40]
        self.alertRecord = json.loads(alertFN.to_json(orient='records'))
        dfinal['f_date'] = pd.to_datetime(dfinal['timestamp']).dt.tz_convert(None)

        self.processedData = json.loads(dfinal.to_json(orient='records'))

        
        file_path = "/home/hafizaimanhassan/airflow/image/api_bq/alert.json"
        if os.path.isfile(file_path):
            os.remove(file_path)
        with open(file_path, 'a', encoding='utf8') as outfile:
            alertList = json.dumps(self.alertRecord)
            outfile.write( alertList )

    def upload_to_bq(self):
        """ Upload processed data to BigQuery """
        try:
            # project_id = 'radiant-micron-790'
            # dataset_id = 'TEMP_DE'
            # table_name = 'ride_STG_2'
            print(f'Saving to BQ path... : {self.project_id}.{self.dataset_id}.{self.table_name}')

            #table_name = 'radiant-micron-790.TEMP_DE.ride_STG'

            client = bigquery.Client(
                project= self.project_id,
                #location= location,
            )

            table = f'{self.project_id}.{self.dataset_id}.{self.table_name}'

            with open(f'/home/hafizaimanhassan/airflow/image/api_bq/schema/ride.json', 'r') as openfile:
                schema_obj = json.load(openfile)

            job_config_params ={
                'schema' : [
                    bigquery.SchemaField( # non-nested field
                        name=i.get('name',''),
                        field_type=i.get('field_type',''),
                        mode=i.get('mode',''),
                        fields=()
                    ) if 'fields' not in i else \
                    bigquery.SchemaField( # nested field
                        name=i.get('name',''),
                        field_type=i.get('field_type',''),
                        mode=i.get('mode',''),
                        fields=[bigquery.SchemaField(**field_details) for field_details in i.get('fields','')],
                    ) \
                    for i in schema_obj
                ],
                'create_disposition': bigquery.CreateDisposition.CREATE_IF_NEEDED
            }

            if self.write_disposition == 'WRITE_TRUNCATE':
                _write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
            elif self.write_disposition == 'WRITE_APPEND':
                _write_disposition = bigquery.WriteDisposition.WRITE_APPEND
            else:
                raise SourceError('Invalid WriteDisposition method is provided.')

            job_config_params['write_disposition'] = _write_disposition

            job_config = bigquery.LoadJobConfig(**job_config_params)

            load_job = client.load_table_from_json(
                json_rows=self.processedData,
                destination=table,
                project=self.project_id,
                #location=self.location,
                job_config=job_config,
            ) # Make an API request.

            load_job.result() # Waits for the job to complete.
            print(f'{len(self.processedData)} new records are uploaded to {table}.')
            print(self.alertRecord)

        except Exception as err:
            raise SourceError(err)


