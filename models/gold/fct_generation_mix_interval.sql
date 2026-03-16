-- models/gold/fct_generation_mix_interval.sql
-- Region × technology × interval generation and emissions for dashboard consumption.

with dispatch as (
    select * from {{ ref('silver_dispatch_interval') }}
    where not dq_flag
),

generators as (
    select * from {{ ref('dim_generator') }}
),

factors as (
    select * from {{ ref('silver_emissions_factor') }}
),

joined as (
    select
        d.settlement_date,
        g.region,
        g.technology_type,
        d.scada_mw * (5.0 / 60.0) as generation_mwh,
        d.scada_mw * (5.0 / 60.0) * coalesce(f.ef_scope1_tco2e_mwh, 0) as scope1_tco2e,
        d.scada_mw * (5.0 / 60.0) * coalesce(f.ef_scope3_tco2e_mwh, 0) as scope3_tco2e
    from dispatch d
    inner join generators g on d.duid = g.duid
    left join factors f on g.technology_type_key = f.technology_type_key
    where g.region is not null
),

aggregated as (
    select
        settlement_date,
        region,
        technology_type,
        sum(generation_mwh) as generation_mwh,
        sum(scope1_tco2e) as scope1_tco2e,
        sum(scope3_tco2e) as scope3_tco2e,
        sum(scope1_tco2e + scope3_tco2e) as total_tco2e
    from joined
    group by settlement_date, region, technology_type
)

select
    settlement_date,
    region,
    technology_type,
    round(generation_mwh, 4) as generation_mwh,
    round(scope1_tco2e, 6) as scope1_tco2e,
    round(scope3_tco2e, 6) as scope3_tco2e,
    round(total_tco2e, 6) as total_tco2e
from aggregated
where generation_mwh > 0
