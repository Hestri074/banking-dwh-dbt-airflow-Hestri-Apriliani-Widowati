-- models/stagging/stg_account.sql
{{ config(materialized='view') }}

select
    account_id                                      as account_id,
    customer_id                                     as customer_id,
    account_number                                  as account_number,
    lower(account_type)                             as account_type,
    account_status                                  as account_status,
    cast(open_date as date)                         as open_date,
    cast(close_date as date)                        as close_date,
    branch_id                                       as branch_id,
    branch_name                                     as branch_name,
    cast(interest_rate as double)                   as interest_rate,
    minimum_balance                                 as minimum_balance,
    current_balance                                 as current_balance,
    currency                                        as currency,
    cast(last_transaction_date as date)             as last_transaction_date
from {{ ref('accounts') }}
