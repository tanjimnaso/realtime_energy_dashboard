-- models/gold/fct_regional_emissions_intensity.sql
-- Regional emissions intensity per 5-minute dispatch interval.
--
-- Calculation:
--   total_tCO2e = sum(scada_mw * (5/60) * emission_factor_tCO2e_MWh)
--   intensity   = (total_tCO2e / total_generation_mwh) * 1000  [gCO2eq/kWh]
--
-- Only intervals where total_generation_mwh > 0 are included.

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
        -- Energy generated this interval in MWh (5-minute window)
        d.scada_mw * (5.0 / 60.0)                         as generation_mwh,
        -- CO2e emitted this interval in tonnes
        d.scada_mw * (5.0 / 60.0) * coalesce(f.emission_factor_tCO2e_MWh, 0) as tCO2e
    from dispatch d
    inner join generators g on d.duid = g.duid
    left join factors f on g.technology_type_key = f.technology_type_key
    where g.region is not null
),

-- Aggregate to region × interval
aggregated as (
    select
        settlement_date,
        region,
        sum(generation_mwh)  as total_generation_mwh,
        sum(tCO2e)           as total_tCO2e
    from joined
    group by settlement_date, region
),

final as (
    select
        settlement_date,
        region,
        round(total_generation_mwh, 4) as total_generation_mwh,
        round(total_tCO2e, 6)          as total_tCO2e,
        -- Intensity in gCO2eq/kWh (tonnes/MWh * 1000 = g/kWh)
        round((total_tCO2e / nullif(total_generation_mwh, 0)) * 1000, 2)
            as emissions_intensity_gCO2eq_per_kWh
    from aggregated
    where total_generation_mwh > 0
)

select * from final
