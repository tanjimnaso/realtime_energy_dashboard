-- models/silver/silver_emissions_factor.sql
-- Scope 1 emissions factors only, with a normalised join key.
-- Scope 3 factors exist in Bronze but are excluded from the intensity calculation.

with source as (
    select * from {{ source('bronze', 'emissions_factors') }}
),

scope1 as (
    select
        lower(trim(technology_type))       as technology_type_key,
        trim(technology_type)              as technology_type,
        cast(emission_factor_tCO2e_MWh as double) as emission_factor_tCO2e_MWh
    from source
    where scope = 'scope_1'
)

select * from scope1
