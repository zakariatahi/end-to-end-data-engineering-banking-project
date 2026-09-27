{{ config(materialized='view') }}

with ranked_events as (
    select
        v:id::number(38, 0) as transaction_id,
        v:account_id::number(38, 0) as account_id,
        v:txn_type::string as transaction_type,
        v:amount::number(18, 2) as amount,
        v:related_account_id::number(38, 0) as related_account_id,
        v:status::string as status,
        try_to_timestamp_ntz(v:created_at::string) as transaction_time,
        v:_op::string as change_type,
        v:_kafka_offset::number(38, 0) as kafka_offset,
        row_number() over (
            partition by v:id::number(38, 0)
            order by v:_kafka_offset::number(38, 0) desc
        ) as event_rank
    from {{ source('raw', 'transactions') }}
)

select
    transaction_id,
    account_id,
    transaction_type,
    amount,
    related_account_id,
    status,
    transaction_time,
    change_type,
    kafka_offset
from ranked_events
where event_rank = 1
  and change_type != 'd'
