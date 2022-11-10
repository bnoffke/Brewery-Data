{% set beverage_types = ["IPA","Stout","Sour","Saison","Ale","Lager","Pilsner","Selzter","Hefeweizen","Tripel","Dubbel","Quad"] %}

with beverages as (
    select *
    from {{ ref('beverages_incremental') }}
)

select stg_beverages.brewery_id,
    count(beverages.id) num_Beverages,
    {%for beverage_type in beverage_types %}
    count(case when beverages.beverage_type = '{{beverage_type}}' then beverages.id else null end) num_{{beverage_type}}s
    {% endfor %}
from beverages
group by stg_beverages.brewery_id