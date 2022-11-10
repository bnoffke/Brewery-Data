{{
    config(
        materialized='ephemeral'
        )
}}


select *
from `brewery-data-367214`.`brewData`.`staging_beverages`