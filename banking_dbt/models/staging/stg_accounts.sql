{{ config(materialized='view') }}

with ranked_events as (
    select
        v:id::number(38, 0) as account_id,
        v:customer_id::number(38, 0) as customer_id,
        v:account_type::string as account_type,
        v:balance::number(18, 2) as balance,
        v:currency::string as currency,
        try_to_timestamp_ntz(v:created_at::string) as created_at,
        v:_op::string as change_type,
        v:_kafka_offset::number(38, 0) as kafka_offset,
        row_number() over (
            partition by v:id::number(38, 0)
            order by v:_kafka_offset::number(38, 0) desc
        ) as event_rank
    from {{ source('raw', 'accounts') }}
)

select
    account_id,
    customer_id,
    account_type,
    balance,
    currency,
    created_at,
    change_type,
    kafka_offset
from ranked_events
where event_rank = 1
  and change_type != 'd'
