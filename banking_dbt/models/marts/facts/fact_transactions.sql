{{ config(materialized='incremental', unique_key='transaction_id', incremental_strategy='merge') }}

select
    t.transaction_id,
    t.account_id,
    a.customer_id,
    t.transaction_type,
    t.amount,
    t.related_account_id,
    t.status,
    t.transaction_time,
    t.kafka_offset
from {{ ref('stg_transactions') }} as t
left join {{ ref('stg_accounts') }} as a
    on t.account_id = a.account_id
{% if is_incremental() %}
where t.kafka_offset > (
    select coalesce(max(kafka_offset), -1)
    from {{ this }}
)
{% endif %}
