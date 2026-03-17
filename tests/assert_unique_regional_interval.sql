-- Fails if there is more than one regional intensity row per settlement_date + region.

select
    settlement_date,
    region,
    count(*) as row_count
from {{ ref('fct_regional_emissions_intensity') }}
group by settlement_date, region
having count(*) > 1
