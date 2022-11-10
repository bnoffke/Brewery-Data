{{
    config(
        materialized='incremental',
        unique_key='id'
        )
}}



with beverages as (
    select *
    from {{ ref('stg_beverages') }}
)

{% if is_incremental() %}
    where beverages.updated_at > ifnull(
            (
            select max(dataModel.updated_at) 
            from {{ this }} as dataModel
            where beverages.id = dataModel.id
            )
        ,'1900-12-31')
        or beverages.id not in (
            select dataModel.id
            from {{ this }} as dataModel
        )
{% endif %}
