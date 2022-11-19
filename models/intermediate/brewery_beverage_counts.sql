{% set beverage_types = ["IPA","Stout","Sour","Saison","Ale","Lager","Pilsner","Selzter","Hefeweizen","Tripel","Dubbel","Quad"] %}

with beverages as (
    select *
    from {{ ref('beverages_incremental') }}
)

select brewery_id,
    count(id) num_beverages,
    {%for beverage_type in beverage_types %}
    count(case when beverage_type = '{{beverage_type}}' then id else null end) num_{{ dbt_utils.slugify(beverage_type) }}s,
    {% endfor %}
from beverages
group by brewery_id