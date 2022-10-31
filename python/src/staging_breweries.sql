--Drop the table because WRITE_TRUNCATE replaces the table and interfers with the table's schema as defined here
DROP TABLE IF EXISTS brewData.staging_breweries;

CREATE OR REPLACE TABLE brewData.staging_breweries 
(
    id STRING NOT NULL OPTIONS(description="The primary key for the table"),
    name STRING OPTIONS(description="The name of the brewery"),
    brewery_type STRING OPTIONS(description="The type of brewery"),
    street STRING OPTIONS(description="The street address for the brewery"),
    address_2 STRING OPTIONS(description="The 2nd address line for the brewery"),
    address_3 STRING OPTIONS(description="The 3rd address line for the brewery"),
    city STRING OPTIONS(description="The city of the brewery"),
    state STRING OPTIONS(description="The state the brewery is located in"),
    county_province STRING OPTIONS(description="The county or province the brewery is located in"),
    postal_code STRING OPTIONS(description="The postal code of the brewery"),
    country STRING OPTIONS(description="The country of the brewery"),
    longitude FLOAT64 OPTIONS(description="The longitude of the brewery's location"),
    latitude FLOAT64 OPTIONS(description="The latitude of the brewery's location"),
    phone STRING OPTIONS(description="The phone number of the brewery"),
    website_url STRING OPTIONS(description="The URL of the website for the brewery"),
    updated_at DATETIME OPTIONS(description="The timestamp this record was updated on Open Brewery DB"),
    created_at DATETIME OPTIONS(description="The timestamp this record was created on Open Brewery DB"),
);