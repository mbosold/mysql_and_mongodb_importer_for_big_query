import os
general = {
 'log_file_folder': 'C:\\Users\\<username>\\<logfiles_folder>', # folder for error logs
 'python_script_name': 'import.py', # script name for error log entries
 'csv_cache_folder': 'C:\\Users\\<username>\\<cache_folder>\\' # folder for local csv exports
}

mysql = {'dbs' :[
 {
  'name': 'db1name', # mysql database name
  'host': os.getenv('db1host'), # mysql host stored as windows environment variable
  'user': os.getenv('db1user'), # mysql user stored as windows environment variable
  'pw': os.getenv('db1pw'), # mysql password stored as windows environment variable
  'use_table_whitelist': True,  # use table whitelist for this database
  'use_field_whitelist': False,  # use field whitelist for this database
  'use_field_blacklist': True,  # use field blacklist for this database
  'table_whitelist': [ # mysql tables for sync
   'table_name1',
   'table_name2'
   ],
  'field_whitelist': [
   'table_name1.field1',
   'table_name1.field2',
   'table_name2.field1',
   'table_name2.field2'
   ],
  'field_blacklist': [
   'table_name1.field1',
   'table_name1.field2',
   'table_name2.field1',
   'table_name2.field2'
   ]
 },
 {
  'name': 'db2name',
  'host': os.getenv('db2host'),
  'user': os.getenv('db2user'),
  'pw': os.getenv('db2pw'),
  'use_table_whitelist': True,
  'use_field_whitelist': False,
  'use_field_blacklist': True,
  'table_whitelist': [
   'table_name1',
   'table_name2'
   ],
  'field_whitelist': [
   'table_name1.field1',
   'table_name1.field2',
   'table_name2.field1',
   'table_name2.field2'
   ],
  'field_blacklist': [
   'table_name1.field1',
   'table_name1.field2',
   'table_name2.field1',
   'table_name2.field2'
   ]
 }
]}

mongo = {'dbs' :[
 {
  'name': 'db1name', # mongoDB database name
  'host': os.getenv('db1host'), # mongoDB host stored as windows environment variable
  'user': os.getenv('db1user'), # mongoDB user stored as windows environment variable
  'pw': os.getenv('db1pw'), # mongoDB password stored as windows environment variable
  'use_collection_whitelist': True,  # use collection whitelist for this database
  'use_field_whitelist': False,  # use field whitelist for this database
  'use_field_blacklist': True,  # use field blacklist for this database
  'collection_whitelist': [ # mysql tables for sync
   'collection_1',
   'collection_2'
   ],
  'field_whitelist': [
   'table_name1.field1',
   'table_name1.field2',
   'table_name2.field1',
   'table_name2.field2'
   ],
  'field_blacklist': [
   'table_name1.field1',
   'table_name1.field2',
   'table_name2.field1',
   'table_name2.field2'
   ]
 },
 {
  'name': 'db2name',
  'host': os.getenv('db2host'),
  'user': os.getenv('db2user'),
  'pw': os.getenv('db2pw'),
  'use_table_whitelist': True,
  'use_field_whitelist': False,
  'use_field_blacklist': True,
  'collection_whitelist': [ # mysql tables for sync
   'collection_1',
   'collection_2'
   ],
  'field_whitelist': [
   'collection_1.field1',
   'collection_1.field2',
   'collection_2.field1',
   'collection_2.field2'
   ],
  'field_blacklist': [
   'collection_1.field1',
   'collection_1.field2',
   'collection_2.field1',
   'collection_2.field2'
   ]
 }
]}

gcp = {
 'credentialfile': os.path.join('C:\\Users\\<username>\\<keyfile_folder>',r'creds.json'), # location of JSON Key File for Google Cloud
 'project_id': '<project-id>', # set Google Cloud Project ID
 'bucket_name': '<bucket_name>', # set Google Cloud Storage Bucket Names for CSV Exports
 'dataset_id': '<dataset_id>' # set Google BigQuery Datatset for CSV Imports
}

slack_hooks = {
 'slack_channel_1': os.getenv('<slack_channel_1>') # stored as windows environment variable
}
