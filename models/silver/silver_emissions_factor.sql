-- models/silver/silver_emissions_factor.sql
-- Scope 1 and Scope 3 emissions factors, normalised to one row per technology type.

with source as (
    select * from {{ source('bronze', 'emissions_factors') }}
),

normalised as (
    select
        lower(trim(technology_type))       as technology_type_key,
        trim(technology_type)              as technology_type,
        trim(scope)                        as scope,
        cast(emission_factor_tCO2e_MWh as double) as emission_factor_tCO2e_MWh
    from source
),

pivoted as (
    select
        technology_type_key,
        max(technology_type) as technology_type,
        max(case when scope = 'scope_1' then emission_factor_tCO2e_MWh end) as ef_scope1_tco2e_mwh,
        max(case when scope = 'scope_3' then emission_factor_tCO2e_MWh end) as ef_scope3_tco2e_mwh
    from normalised
    group by technology_type_key
)

select * from pivoted
