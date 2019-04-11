# coding: utf-8
# Import nessary libaries and load config file
from sqlalchemy import create_engine
from google.cloud import storage
from google.cloud import bigquery
from urllib import request, parse
from datetime import datetime
from pymongo import MongoClient
import json
import csv
import pandas as pd
import os
import datalab.storage as gcs
import import_config as config
import pymongo
import numpy as np
import sys

# Function to connect to MySQL database, creates engine, create Panda dataframes for all tables, transform data and creates csv files in Google Cloud Storage
def mysql_to_gcs():
    # Iterate through all existing mysql databases of config file
    for idx, mysql_database in enumerate(config.mysql['dbs']):
        # Connect to mysql db and load table and field whitelist and blacklist configuration
        mysql_client_url = 'mysql+mysqldb://'+mysql_database['user']+':'+mysql_database['pw']+'@'+mysql_database['host']+"/"+mysql_database['name']
        mysql_engine = create_engine(mysql_client_url)
        mysql_table_names = mysql_engine.table_names()
        use_field_whitelist = mysql_database['use_field_whitelist']
        field_whitelist = mysql_database['field_whitelist']
        use_field_blacklist = mysql_database['use_field_blacklist']
        field_blacklist = mysql_database['field_blacklist']
        # Iterate through all tables of current mysql database
        for mysql_table in mysql_table_names:
            # Check if whitelisting is active and if current table is listed in the whitelist
            if (mysql_database['use_table_whitelist'] == False) or (mysql_database['use_table_whitelist']== True and mysql_table in mysql_database['table_whitelist']):
                # Select table data of mysql db
                query='SELECT * FROM '+mysql_table
                # Google BigQuery doesnt allow "-" in table names, so we replace it with underscores
                db_filename = mysql_database['name'].replace("-", "_")
                # Create file names and paths for CSV and JSON export files
                file_name_csv = 'mysql_'+db_filename+'_'+mysql_table+'.csv'
                file_name_json = 'mysql_'+db_filename+'_'+mysql_table+'.json'
                file_name_csv_path=os.path.join(config.general['csv_cache_folder'],file_name_csv)
                file_name_json_path=os.path.join(config.general['csv_cache_folder'],file_name_json)
                # Create dataframe for table
                table_data = pd.read_sql_query(query, mysql_engine)
                # Move to next table if MySQL tables is empty
                if len(table_data.index) == 0:
                    continue
                # Transform dataframe with given function
                table_data = transform_dataframe(table_data,mysql_table,use_field_whitelist,field_whitelist,use_field_blacklist,field_blacklist)
                # Create BigQuery data schema for transformed dataframe with given function
                table_data_schema = generate_bq_schema(table_data, default_type='STRING')
                # Local export of csv file
                table_data.to_csv(file_name_csv_path, sep='|', quoting=csv.QUOTE_NONNUMERIC, index=False, encoding='utf8')
                # Local export to json file
                with open(file_name_json_path, 'w') as fp:
                    json.dump(table_data_schema, fp)
                # Upload local CSV File to Google Cloud Storage
                upload_to_google_cloud_storage(file_name_csv,file_name_csv_path)

# Function to connect to MongoDB database, creates engine, create Panda dataframes for all tables, transform data and creates csv files in Google Cloud Storage
def mongo_to_gcs():
    # Iterate through all existing MongoDB databases of config file
    for idx, mongo_database in enumerate(config.mongo['dbs']):
        # Connect to MongoDB and load collection and field whitelist and blacklist configuration
        mongo_client_url = 'mongodb+srv://'+mongo_database['user']+':'+mongo_database['pw']+'@'+mongo_database['host']
        mongo_client = pymongo.MongoClient(mongo_client_url, 27017)
        mongo_db = mongo_client[mongo_database['name']]
        mongo_collection_names = mongo_db.list_collection_names()
        use_field_whitelist = mongo_database['use_field_whitelist']
        field_whitelist = mongo_database['field_whitelist']
        use_field_blacklist = mongo_database['use_field_blacklist']
        field_blacklist = mongo_database['field_blacklist']
        # Iterate through all collections of MongoDB database
        for mongo_collection in mongo_collection_names:
            # Check if whitelisting is active and if current collection is listed in the whitelist
            if (mongo_database['use_collection_whitelist'] == False) or (mongo_database['use_collection_whitelist']== True and mongo_collection in mongo_database['collection_whitelist']):
                # Select collection of mongo db
                mongo_collection_client = mongo_db[mongo_collection]
                cursor = mongo_collection_client.find({})
                # Google BigQuery doesnt allow "-" in table names, so we replace it with underscores
                db_filename = mongo_database['name'].replace("-", "_")
                # Create file names and paths for CSV and JSON export files
                file_name_csv = 'mongo_'+db_filename+'_'+mongo_collection+'.csv'
                file_name_csv_path=os.path.join(config.general['csv_cache_folder'],file_name_csv)
                file_name_json = 'mongo_'+db_filename+'_'+mongo_collection+'.json'
                file_name_json_path=os.path.join(config.general['csv_cache_folder'],file_name_json)
                # Create dataframe for collection
                table_data = pd.DataFrame(list(cursor))
                # Transform dataframe with given function
                table_data = transform_dataframe(table_data,mongo_collection,use_field_whitelist,field_whitelist,use_field_blacklist,field_blacklist)
                # Create BigQuery data schema for transformed dataframe with given function
                table_data_schema = generate_bq_schema(table_data, default_type='STRING')
                # Local export of csv file
                table_data.to_csv(file_name_csv_path, sep='|', quoting=csv.QUOTE_NONNUMERIC, float_format='%.2f', index=False, encoding='utf8')
                # Local export to json file
                with open(file_name_json_path, 'w') as fp:
                    json.dump(table_data_schema, fp)
                # Upload local CSV File to Google Cloud Storage
                upload_to_google_cloud_storage(file_name_csv,file_name_csv_path)

# Function to read all objects of a given Google Cloud Storage Bucket and create Google BigQuery tables in a dataset for each
def gcs_to_gbq():
    # Connect to Google Cloud Storage and set bucket
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(config.gcp['bucket_name'])
    blobs = bucket.list_blobs()
    client = bigquery.Client()
    # Connect to Google BigQuery and define BigQuery options
    dataset_ref = client.dataset(config.gcp['dataset_id'])
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    job_config.allow_quoted_newlines = True
    job_config.allow_jagged_rows = True
    job_config.autodetect = False
    job_config.skip_leading_rows = 1
    job_config.field_delimiter = "|"
    job_config.source_format = bigquery.SourceFormat.CSV
    # Iterate though all files of defined Google Cloud Storage bucket
    for blob in blobs:
        # Set dynamic URL for current Cloud Storage file and BigQuery schema file
        uri = 'gs://'+config.gcp['bucket_name']+'/'+blob.name
        file_name_json = blob.name.replace(".csv",".json")
        file_name_json_path=os.path.join(config.general['csv_cache_folder'],file_name_json)
        # Load JSON File for schema and set schema fields for BigQuery
        input_json = open(file_name_json_path)
        input_json_config = json.load(input_json)
        job_config.schema = [
             bigquery.SchemaField(item["name"], item["type"]) for item in input_json_config["fields"]
        ]
        # Set dynamic table name for Google BigQuery
        table_name = blob.name.replace(".csv", "")
        # Create new big query table / replace existing
        load_job = client.load_table_from_uri(uri, dataset_ref.table(table_name), job_config=job_config)
        assert load_job.job_type == 'load'
        load_job.result()  # Waits for table load to complete.
        assert load_job.state == 'DONE'
    # Send slack message when load job done
    send_message_to_slack(':bigquery: Backend Data Sync: Update DWH Data OK :thumbs-up-green:', config.slack_hooks['slack_channel_1'])

# Function to create dynamic Google BigQuery data schema from pandas dataframe types
# Create mapping between Pandas dataframe types and Google BigQuery field types
def generate_bq_schema(dataframe, default_type='STRING'):
    type_mapping = {
        'i': 'INTEGER',
        'b': 'BOOLEAN',
        'f': 'FLOAT',
        'O': 'STRING',
        'S': 'STRING',
        'U': 'STRING',
        'M': 'TIMESTAMP'
    }
    # Use the mapping and create BigQuery schema object for dataframe
    fields = []
    for column_name, dtype in dataframe.dtypes.iteritems():
        fields.append({'name': column_name,
                       'type': type_mapping.get(dtype.kind, default_type)})
    return {'fields': fields}

# Function to apply column whitelisting/blacklisting and prepare fields for BigQuery upload
def transform_dataframe(dataframe,table_name,use_field_whitelist,field_whitelist,use_field_blacklist,field_blacklist):
    # Iterate through all existing columns of current dataframe
    for column in dataframe:
        table_column_name = table_name+"."+column
        # use this function to export all existing columns of whitelisted database tables with the next line
        # export_table_fields(table_column_name)
        # When whitelist is activated for the database, drop all fields, which are not in whitelist
        if (use_field_whitelist == True and table_column_name not in field_whitelist):
            dataframe.drop(column, axis=1, errors='ignore', inplace=True)
            continue
        # When blacklist is activated for the database, drop all fields, which are in blacklist
        if (use_field_blacklist == True and table_column_name in field_blacklist):
            dataframe.drop(column, axis=1, errors='ignore', inplace=True)
            continue
        # Find all fields with '\n' and '\r' and replace them with white spaces
        if dataframe[column].dtype != np.number:
            dataframe[column].replace('\n',' ', inplace=True, regex=True)
            dataframe[column].replace('\r',' ', inplace=True, regex=True)
    # Find all fields, where the fieldname starts with "_" and remove the first character from the field name
    dataframe.rename(columns=lambda x: x[1:] if (x[0]=='_') else x, inplace=True)
    return dataframe

# Function to send success or fail messages to configured slack channel for monitoring reasons
def send_message_to_slack(alert_message, slack_hook):
    post = {"text": "{0}".format(alert_message)}
    json_data = json.dumps(post)
    req = request.Request(slack_hook, data=json_data.encode('ascii'), headers={'Content-Type': 'application/json'})
    resp = request.urlopen(req)

# Function to write error to a local log file
def write_error_to_log(error_message):
    i = datetime.now()
    logfile_name = 'log-'+i.strftime('%Y-%m-%d')+'.txt'
    logfile_path = os.path.join(config.general['log_file_folder'],logfile_name)
    logfile = open(logfile_path,'a+')
    logfile.write(i.strftime('%Y-%m-%d %H:%M:%S')+' '+config.general['python_script_name']+': '+error_message+'\n')

# Function to upload files to Google CLoud Storage bucket from config
def upload_to_google_cloud_storage(file_name_csv,file_name_csv_path):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(config.gcp['bucket_name'])
    blob = bucket.blob(file_name_csv)
    blob.upload_from_filename(file_name_csv_path)

# Help function to write all fields of whitelisted tables to a local log file for copy & pasting to fields whitelist of config file
def export_table_fields(field):
    logfile_name = 'fields.txt'
    logfile_path = os.path.join(config.general['csv_cache_folder'],logfile_name)
    logfile = open(logfile_path,'a+')
    logfile.write(field+'\n')

# Execution of complete job
try:
    # execute mysql_to_gcs function for all mysql databases
    mysql_to_gcs()
    # execute mongo_to_gcs function for all mongo databases
    mongo_to_gcs()
    # execute gcs_to_gbq function for google cloud storage buckets
    gcs_to_gbq()
except Exception as em:
    # Send message to slack for failed job and write error log
    send_message_to_slack(':bigquery: Backend Data Sync: Update DWH Data Failed :no_entry:', config.slack_hooks['slack_channel_1'])
    write_error_to_log(str(em))
