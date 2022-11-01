{{
    config(
        materialized='incremental',
        unique_key='id'
        )
}}

select stage.*
from `brewery-data-367214`.`brewData`.`staging_breweries` stage

{% if is_incremental() %}
    where updated_at > ifnull(
            (
            select max(dataModel.updated_at) 
            from {{ this }} as dataModel
            where stage.id = dataModel.id
            )
        ,'1900-12-31')
        or stage.id not in (
            select dataModel.id
            from {{ this }} as dataModel
        )
{% endif %}