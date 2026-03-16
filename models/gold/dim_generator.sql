-- models/gold/dim_generator.sql
-- Dimension table: one row per DUID with technology, region, and dispatch type.

select
    duid,
    unit_name,
    technology_type,
    lower(technology_type) as technology_type_key,
    region,
    dispatch_type
from {{ ref('silver_generator_metadata') }}
