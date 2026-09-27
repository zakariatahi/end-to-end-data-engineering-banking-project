{{ config(materialized='view') }}

with ranked_events as (
    select
        v:id::number(38, 0) as customer_id,
        v:first_name::string as first_name,
        v:last_name::string as last_name,
        v:email::string as email,
        try_to_timestamp_ntz(v:created_at::string) as created_at,
        v:_op::string as change_type,
        v:_kafka_offset::number(38, 0) as kafka_offset,
        row_number() over (
            partition by v:id::number(38, 0)
            order by v:_kafka_offset::number(38, 0) desc
        ) as event_rank
    from {{ source('raw', 'customers') }}
)

select
    customer_id,
    first_name,
    last_name,
    email,
    created_at,
    change_type,
    kafka_offset
from ranked_events
where event_rank = 1
  and change_type != 'd'
