-- models/silver/silver_generator_metadata.sql
-- Normalised generator registration metadata, one row per DUID.

with source as (
    select * from {{ source('bronze', 'generator_metadata') }}
),

renamed as (
    select
        trim("DUID")          as duid,
        trim("Unit Name")     as unit_name,
        trim("Technology Type") as technology_type,
        trim("Region")        as region,
        trim("Dispatch Type") as dispatch_type
    from source
    where "DUID" is not null
),

-- Enforce known NEM region codes; anything else is flagged as NULL
conformed as (
    select
        duid,
        unit_name,
        technology_type,
        case
            when region in ('NSW1','VIC1','QLD1','SA1','TAS1') then region
            else null
        end as region,
        dispatch_type
    from renamed
),

deduped as (
    select *
    from conformed
    qualify row_number() over (partition by duid order by duid) = 1
)

select * from deduped
