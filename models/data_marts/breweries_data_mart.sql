with breweries as (
    select *
    from {{ ref('breweries_incremental') }}
)

with beverage_counts as (
    select *
    from {{ ref('brewery_beverage_counts') }}
)

select *
from breweries
left outer join beverage_counts
    on breweries.id = beverage_counts.brewery_id