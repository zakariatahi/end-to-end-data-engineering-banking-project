{{ config(materialized='table') }}

select
    account_id,
    customer_id,
    account_type,
    balance,
    currency,
    created_at,
    dbt_valid_from as effective_from,
    dbt_valid_to as effective_to,
    dbt_valid_to is null as is_current
from {{ ref('accounts_snapshot') }}
