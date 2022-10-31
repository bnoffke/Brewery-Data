from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import json
import requests
from pandas.io import gbq
import datetime
from dateutil import parser

key_path = './brewery-data-local-machine-key.json'

# Construct a BigQuery client object.
credentials = service_account.Credentials.from_service_account_file(
    key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
)

#Start a client to connect to our BQ project
client = bigquery.Client(credentials=credentials, project=credentials.project_id,)

#Grab the SQL to create the staging table
with open ('./python/src/staging_breweries.sql','r') as create_table_file:
    create_table = create_table_file.read()
create_table_file.close()
query_job = client.query(create_table)
query_job.result()

#Prepare to pull data from API
url = 'https://api.openbrewerydb.org/breweries?by_state=wisconsin&per_page=50'
r = requests.get(url)
data = r.json()

df = pd.DataFrame.from_dict(data)

colTypes = {'id': str,
            'name': str,
            'brewery_type': str,
            'street': str,
            'address_2': str,
            'address_3': str,
            'city': str,
            'state': str,
            'county_province': str,
            'postal_code': str,
            'country': str,
            'longitude': float,
            'latitude': float,
            'phone': str,
            'website_url': str
            }
cols = list(colTypes.keys())
strCols = [col for col in colTypes if colTypes[col] == str]

df = df.astype(colTypes)
df.updated_at = df.updated_at.apply(parser.parse)
df.created_at = df.created_at.apply(parser.parse)
df[strCols] = df[strCols].replace('None','')

#Prepare to load data into the staging table
table_id = "brewery-data-367214.brewData.staging_breweries"
job_config = bigquery.LoadJobConfig(
  #write_disposition='WRITE_TRUNCATE',
  source_format = 'CSV'
)

job = client.load_table_from_dataframe(
    df, table_id, job_config=job_config,
)
job.result()  # Wait for the job to complete.

#Check what we loaded
table = client.get_table(table_id)
print(
    "Loaded {} rows and {} columns to {}".format(
        table.num_rows, len(table.schema), table_id
    )
)