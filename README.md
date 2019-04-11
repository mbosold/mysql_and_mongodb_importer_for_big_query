# Selective MySQL and MongoDB data import for Google BigQuery (Windows)

Automatically Load data of multiple MySQL and MongoDB databases to Google BigQuery with the usage of table and column white/blacklisting.

## Disclaimer

* I do not consider myself a full-time programmer. I create this Python script to solve a specific problem for one of my clients. I’m thankful for every feedback to improve or simplify my code. If you would like to send some feedback, please feel free to email me at contact@datapriest.io
* Code documentation is aligned correctly when the files are viewed in editor “Atom.io”
* I’m aware, that this script might not be the best option for all companies. There are many specialized software vendors (like Stitch or Fivetran) out there, to handle this workload in a managed way with SLAs
* I’m not responsible for any results of this script e.g. bad decisions caused by wrong or bad data imports or infraction of the data privacy laws of your country

## Getting Started

These instructions will get you a copy of the project up and running on your live system.

### How it works
1.	Dynamically read out all tables from multiple MySQL databases and MongoDB databases with separated credentials for each instance and create pandas dataframes for each table
2.	Transform the data inside the dataframes for data privacy and BigQuery requirements (including column renaming, table and field white and black listing, column type adjusting)
3.	Create local CSV files out of the pandas dataframes and local JSON files for the BigQuery data schema
4.	Upload local CSV files to Google Cloud Storage
5.	Upload data from Cloud Storage CSV files to Google BigQuery with the schema configuration of the local JSON files
6.	Send alerts to a defined Slack channel (success or fail)
7.	Write a local log file for errors

### Files

File Name        | Description
---------------- | -------------
import.py        | actual code including all libraries and functions
import_config.py | config file (you need to edit this file)
import.ipynb     | code variant as Jupyter Notebook for easier code editing, if nessary

### Technical Notes

* Don’t use the code without any Python and Windows Server knowledge. Try to understand the code in general to proactively take care about your data quality
* After some internet research, I came to the conclusion, that the fasted way to load bigger datasets to Google BigQuery is to load the data via Google Cloud CSV files. A regular server is able to load 1-2 GBs of data for each table with this script.
* For data privacy reasons, you should use the table and field whitelist feature. Therefore, it’s not recommended to sync all database tables including passwords, credit card details or other personal information without any white or blacklisting
* Google BigQuery will not accept your MySQL and MongoDB data without any further transformation. This is why the transform_dataframe function renames columns, drops empty tables, etc..
* Google BigQuery is able to create data schemas for the tables dynamically (configured by bigquery job settings). Unfortunately, this method is only taking a tiny part of your dataset to account for choosing the “right” field types. This can lead to wrong data types and therefore the function generate_bq_schema create the data fields dynamically out of the pandas dataframe. Unfortunately, Pandas dataframes can’t handle integer fields with NaN values and transform this fields to floats. This means, you might need to transform the related fields back to integers via Google BigQuery SQL
* You should never store your credential information directly in the config file. You should always use an encrypted and secured way to store your credentials

### Known Bugs

* Pandas sets integer fields to float, if NAN values exists, this fields have to be transformed with BigQuery Views

### Requirements

* OS: Windows (tested with Windows Server 2016 Datacenter)
* Language: Tested with Python 3.6.0 (Anaconda 4.3.1)
* Packages and Libaries
```
pip install SQLAlchemy
pip install google-cloud
pip install urllib
pip install DateTime
pip install pymongo
pip install pandas
pip install datalab
pip install numpy
```

### Setup
1. clone repository or copy files to your local server
* Remember the file folder path for windows scheduler task (later)

2. Create Google Cloud JSON Key File
* Login into Google Cloud Interface
* Select your Google Cloud Project
* Go to IAM Menu
* Create new IAM with Role “Project – Editor”
* Download JSON key file
* Rename file “creds.json”
* Store key file in one server folder
* Remember the key file folder path for configuration file (later)

3. Create Google Cloud Bucket
* Go to: https://console.cloud.google.com/storage/
* Create new bucket for CSV files
* Remember the bucket name for configuration file (later)

4. Create Google BigQuery Project
* Login into Google Cloud Interface
* If not existing, create new Google BigQuery project
* Remember the BigQuery project id for configuration file (later)

5. Create Google BigQuery Dataset
* Go to: https://bigquery.cloud.google.com/
* Create new data set (you can select EU as location)
* Remember the BigQuery dataset id for configuration file (later)

6. Create local cache folder
* Create a folder for all JSON and CSV files on your Windows server
* Keep in mind, that you need enough space for all (maybe large) csv files
* Remember the cache folder path for configuration file (later)

7. Create local log folder
* Create a folder for log files on your Windows server
* Remember the log folder path for configuration file (later)

7. Store credentials as Windows system variables
* Set environment variables for the database credentials
* Open command prompt on your Windows server
* Use command to set variables e.g.
```setx db1pw “password" /M```
* A restart of your server is required afterwards

8. Setup Slack Incoming Webhook for sending alerts via Slack
* Create an app https://api.slack.com/apps
* Activate “incoming webhooks”
* Add New Webhook to Workspace
* Select Slack channel and save URL
* Set environment variables for the slack webhook url
* Open command prompt on your Windows server
* Use the following comment with your actual slack webhook url
```setx slack_channel_1 “slack_webhook_url" /M```
* A restart of your server is required afterwards

### Configuration with import_config.py (edit file and save)

If the configuration file (import_config) is set up correctly,
there is no further need to change anything in the actual script code (import.py).
As the configuration for all credentials, files, folders, whitelists and blacklists
can be quite complex, you should take your time to understand the architecture of the config file.
Edit the config file to insert the values of the following variables
```
general.log_file_folder,
general.python_script_name [optional],
general.csv_cache_folder,
mysql.dbs.name,
mysql.dbs.host,
mysql.dbs.user,
mysql.dbs.pw,
mysql.dbs.use_table_whitelist,
mysql.dbs.use_field_whitelist,
mysql.dbs.use_field_blacklist,
mysql.dbs.table_whitelist,
mysql.dbs.field_whitelist,
mysql.dbs.field_blacklist,
mongo.dbs.name,
mongo.dbs.host,
mongo.dbs.user,
mongo.dbs.pw,
mongo.dbs.use_collection_whitelist,
mongo.dbs.use_field_whitelist,
mongo.dbs.use_field_blacklist,
mongo.dbs.collection_whitelist,
mongo.dbs.field_whitelist,
mongo.dbs.field_blacklist,
gcp.credentialfile,
gcp.project_id,
gcp.bucket_name,
gcp.dataset_id,
slack_hooks.slack_channel_1
```

### Automate hourly execution of python code with Windows Scheduler

1. Copy “importer.py” and “import_config.py” to folder on Windows Server
2. Open Windows Task Scheduler
3. Click on “Create Basic Task”
4. Enter a name for the new task e.g. “hourly backend import”
5. Enter a trigger like “Daily” –> “Repeat Task every 1 hour for a duration of indefinitely”
6. Select “Action”: “Start a program”
7. Enter path to python.exe for “Program Script”: e.g. “C:\ProgramData\Anaconda3\python.exe”
8. Enter path to python file in “Add arguments”: e.g. "C:\Users\<username>\import.py"
9. Save your new scheduler task

## Own Ideas for further development

* Further modulation e.g. it’s possible to consolidate the functions “mysql_to_gcs” and “mongo_to_gcs”
* Function for email alerts with log details
* Workaround for Pandas handling with integers including NaN values
* Possibility to load data in an incremental way with change keys (not a full load)
* Web GUI instead of configuration file
* Further data sources for data sources like ad network APIs   
* Use Serverless architecture like Google Cloud Functions instead of Windows Server

## Authors

* **Mark Bosold** [Datapriest.io](https://github.com/mbosold)

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details
