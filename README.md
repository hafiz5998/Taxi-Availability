# Taxi-Availability
This is side project that involves GCP- Airflow setup. The main purpose of this project is to extract taxi data from openssource api and load into data warehouse for visualization


There are 2 main folder

dags - Contain .py file for airflow Dag

image - Contain set of code for data ingestion and processing.

-> inside image folder, code is located inside api_bq folder. -> in api_bq, there will be several files

-> the data processing codes is inside folder /lib named as classes.py. All the process and ingestion is prepared in this file.

-> main.py / main_append.py - is a file designed to call up the processing Method from the /lib. Therefore, all the dataset location, mode of table, is specified inside this file.

-> schema folder consist of schema file(.json) - used for uploading data into BigQuery procedure.

-> sg.csv - file that contain list of areas/city with coordinates used for reference

-> alert.json is file that contain list of city under alert condition (to be send through email)

OVERVIEW of the codes methodology

Since each coordinate of Taxi is scattered and every taxi have unique coordinates. Data Clustering operation is applied.

Purpose:

To cluster the coordinates into several clusters
Easier for city/areas classification
Insight and Dashboard Implementation
METHOD (machine learning) :

K-Means Clustering.

To classify and label taxi coordinates into specific cluster
KNeighbors Classifier

Since every taxi coordinates have cluster label. Next, need to define one common coordinate for each cluster for grouping and mapping purpose.
To define each cluster belong to which area/city
A list of areas/city with coordinates from SG website used as reference
Python dataframe (pandas) used for data processing Output of processed data, consist of coordinates, city, timestamp -> ingestion into BigQuery
