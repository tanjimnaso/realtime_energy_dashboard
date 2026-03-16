-- models/silver/silver_dispatch_interval.sql
-- Typed, deduplicated SCADA dispatch intervals with a data-quality flag.

with source as (
    select * from {{ source('bronze', 'dispatch_scada') }}
),

typed as (
    select
        cast(SETTLEMENTDATE as timestamp) as settlement_date,
        trim(DUID)                         as duid,
        cast(SCADAVALUE as double)         as scada_mw
    from source
    where SETTLEMENTDATE is not null
      and DUID is not null
),

deduped as (
    select *
    from typed
    qualify row_number() over (
        partition by settlement_date, duid
        order by settlement_date
    ) = 1
),

final as (
    select
        settlement_date,
        duid,
        scada_mw,
        -- Flag readings that are physically implausible
        (scada_mw < 0 or scada_mw > 5000) as dq_flag
    from deduped
)

select * from final
