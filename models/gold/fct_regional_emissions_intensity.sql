-- models/gold/fct_regional_emissions_intensity.sql
-- Regional emissions intensity per 5-minute dispatch interval for scope 1, scope 3, and total.

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

-- Join dispatch readings to generator metadata and emissions factors
joined as (
    select
        d.settlement_date,
        g.region,
        d.scada_mw,
        d.scada_mw * (5.0 / 60.0)                         as generation_mwh,
        d.scada_mw * (5.0 / 60.0) * coalesce(f.ef_scope1_tco2e_mwh, 0) as scope1_tco2e,
        d.scada_mw * (5.0 / 60.0) * coalesce(f.ef_scope3_tco2e_mwh, 0) as scope3_tco2e,
        d.scada_mw * (5.0 / 60.0) * (
            coalesce(f.ef_scope1_tco2e_mwh, 0) + coalesce(f.ef_scope3_tco2e_mwh, 0)
        ) as total_tco2e
    from dispatch d
    inner join generators g on d.duid = g.duid
    left join factors f on g.technology_type_key = f.technology_type_key
    where g.region is not null
),

aggregated as (
    select
        settlement_date,
        region,
        sum(generation_mwh)  as total_generation_mwh,
        sum(scope1_tco2e)    as scope1_tco2e,
        sum(scope3_tco2e)    as scope3_tco2e,
        sum(total_tco2e)     as total_tco2e
    from joined
    group by settlement_date, region
),

final as (
    select
        settlement_date,
        region,
        round(total_generation_mwh, 4) as total_generation_mwh,
        round(scope1_tco2e, 6)         as scope1_tco2e,
        round(scope3_tco2e, 6)         as scope3_tco2e,
        round(total_tco2e, 6)          as total_tco2e,
        round((scope1_tco2e / nullif(total_generation_mwh, 0)) * 1000, 2)
            as emissions_intensity_scope1_gco2eq_per_kwh,
        round((total_tco2e / nullif(total_generation_mwh, 0)) * 1000, 2)
            as emissions_intensity_total_gco2eq_per_kwh
    from aggregated
    where total_generation_mwh > 0
)

select * from final
