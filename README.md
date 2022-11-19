# Brewery-Data
This is a project to practice Python/dbt/GCP with [Open Brewery DB](https://www.openbrewerydb.org/) data and some fabricated data.

## What does this code do?
The Python scripts will pull from the Open Brewery DB API and load the breweries into a BigQuery staging table. Then, random beverages are generated for the breweries that have been staged and loaded into their own staging table.

The dbt portion transforms the data further by handling incremental updates for breweries/beverages. It also pivots beverage types per breweries to create a summarized data mart for breweries. You can find the documentation for the results data models [here](https://cloud.getdbt.com/accounts/115238/develop/1996893/docs/index.html#!/overview/brewery_data).

## Todo
1. Add error handling in Python code
2. Move python code into a container deployed in GCP Cloud Run, Cloud Function, or Cloud Shell
3. Add unit tests
4. Make list of beverages persistent and support retiring beverages
