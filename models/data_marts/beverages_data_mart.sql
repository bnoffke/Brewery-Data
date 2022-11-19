{{
    config(
        materialized='view'
        )
}}

with beverages as (
    select *
    from {{ ref('beverages_incremental') }}
)

select *
from beverages