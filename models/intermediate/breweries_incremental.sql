{{
    config(
        materialized='incremental',
        unique_key='id'
        )
}}



with breweries as (
    select *
    from {{ ref('stg_breweries') }}
)

{% if is_incremental() %}
    where breweries.updated_at > ifnull(
            (
            select max(dataModel.updated_at) 
            from {{ this }} as dataModel
            where breweries.id = dataModel.id
            )
        ,'1900-12-31')
        or breweries.id not in (
            select dataModel.id
            from {{ this }} as dataModel
        )
{% endif %}
