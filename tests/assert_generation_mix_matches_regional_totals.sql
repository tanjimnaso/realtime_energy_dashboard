-- Fails if the summed generation mix does not reconcile to the regional Gold fact table.

with mix as (
    select
        settlement_date,
        region,
        round(sum(generation_mwh), 4) as generation_mwh,
        round(sum(total_tco2e), 6) as total_tco2e
    from {{ ref('fct_generation_mix_interval') }}
    group by settlement_date, region
),
regional as (
    select
        settlement_date,
        region,
        round(total_generation_mwh, 4) as total_generation_mwh,
        round(total_tco2e, 6) as total_tco2e
    from {{ ref('fct_regional_emissions_intensity') }}
)
select
    coalesce(mix.settlement_date, regional.settlement_date) as settlement_date,
    coalesce(mix.region, regional.region) as region,
    mix.generation_mwh,
    regional.total_generation_mwh,
    mix.total_tco2e as mix_total_tco2e,
    regional.total_tco2e as regional_total_tco2e
from mix
full outer join regional
    on mix.settlement_date = regional.settlement_date
   and mix.region = regional.region
where abs(coalesce(mix.generation_mwh, 0) - coalesce(regional.total_generation_mwh, 0)) > 0.01
   or abs(coalesce(mix.total_tco2e, 0) - coalesce(regional.total_tco2e, 0)) > 0.01
