{{
    config(
        materialized='incremental',
        unique_key='id'
        )
}}

select *
from `brewery-data-367214`.`brewData`.`staging_breweries`

{% if is_incremental() %}
    where updated_at > ifnull((select max(updated_at) from {{ this }} where id = {{this}}.id),'1900-12-31')
{% endif %}