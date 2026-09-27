{{ config(materialized='table') }}

select
    customer_id,
    first_name,
    last_name,
    email,
    created_at,
    dbt_valid_from as effective_from,
    dbt_valid_to as effective_to,
    dbt_valid_to is null as is_current
from {{ ref('customers_snapshot') }}
