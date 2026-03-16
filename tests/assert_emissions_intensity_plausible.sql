-- tests/assert_emissions_intensity_plausible.sql
-- Fails if any regional intensity row is outside the plausible physical range.
-- NEM grid intensity has historically ranged from near 0 (100% renewables)
-- to ~1000 gCO2eq/kWh (heavy coal states). 1500 is a hard upper bound.

select
    settlement_date,
    region,
    emissions_intensity_scope1_gco2eq_per_kwh
from {{ ref('fct_regional_emissions_intensity') }}
where emissions_intensity_scope1_gco2eq_per_kwh < 0
   or emissions_intensity_scope1_gco2eq_per_kwh > 1500
