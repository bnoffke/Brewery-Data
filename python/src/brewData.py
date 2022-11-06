from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import json
import requests
from pandas.io import gbq
import datetime
from dateutil import parser
import generateBeverages as genBev

class BreweryLoader():

    def __init__(self) -> None:
        self.key_path = './brewery-data-local-machine-key.json'
        self.credentials = service_account.Credentials.from_service_account_file(
            self.key_path, scopes=['https://www.googleapis.com/auth/cloud-platform'],
            )
        self.client = bigquery.Client(credentials=self.credentials, project=self.credentials.project_id,)

        self.breweries = pd.DataFrame()
    
    def prepStagingTable(self,tableDefSql):
        with open (tableDefSql,'r') as create_table_file:
            create_table = create_table_file.read()
        create_table_file.close()
        query_job = self.client.query(create_table)
        query_job.result()
    
    def getBreweries(self,urlBase):
        #Prepare to pull data from API
        url = urlBase + '&per_page=50'
        page = 1
        r = requests.get(url)
        data = r.json()
        #Loop through pages of API calls and append results to our dataframe
        while len(data) > 0:
            self.breweries = pd.concat([self.breweries,pd.DataFrame.from_dict(data)])
            page += 1
            r = requests.get(url + f'&page={page}')
            data = r.json()
    
    def cleanBreweriesDataTypes(self):
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

        self.breweries = self.breweries.astype(colTypes)
        self.breweries.updated_at = self.breweries.updated_at.apply(parser.parse)
        self.breweries.created_at = self.breweries.created_at.apply(parser.parse)
        self.breweries[strCols] = self.breweries[strCols].replace('None','')

    def getBeverages(self):
        genBev.assignBeverages(self.breweries)
        self.beverages = genBev.makeBeveragesDataFrame()


    def loadData(self,table_id):
        #Prepare to load data into the staging table
        #table_id = "brewery-data-367214.brewData.staging_breweries"
        job_config = bigquery.LoadJobConfig(source_format = 'CSV')
        
        if table_id.split('.')[2] == 'staging_breweries':
            df = self.breweries
        elif table_id.split('.')[2] == 'staging_beverages':
            df = self.beverages

        job = self.client.load_table_from_dataframe(
            df, table_id, job_config=job_config,
        )
        job.result()  # Wait for the job to complete.

        #Check what we loaded
        table = self.client.get_table(table_id)
        print(
            "Loaded {} rows and {} columns to {}".format(
                table.num_rows, len(table.schema), table_id
            )
        )

def main():
    myBreweryLoader = BreweryLoader()
    myBreweryLoader.getBreweries(urlBase = 'https://api.openbrewerydb.org/breweries?by_state=wisconsin')
    myBreweryLoader.cleanBreweriesDataTypes()
    myBreweryLoader.prepStagingTable(tableDefSql = './python/src/staging_breweries.sql')
    myBreweryLoader.loadData(table_id = 'brewery-data-367214.brewData.staging_breweries')

    myBreweryLoader.getBeverages()
    myBreweryLoader.prepStagingTable(tableDefSql = './python/src/staging_beverages.sql')
    myBreweryLoader.loadData(table_id = 'brewery-data-367214.brewData.staging_beverages')


if __name__ == '__main__':
    main()