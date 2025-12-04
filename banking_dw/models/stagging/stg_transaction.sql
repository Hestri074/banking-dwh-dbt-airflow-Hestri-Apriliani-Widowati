-- models/stagging/stg_transaction.sql
{{ config(materialized='view') }}

select
    transaction_id                              as transaction_id,
    account_id                                  as account_id,
    cast(transaction_date as date)              as transaction_date,
    cast(transaction_time as time)              as transaction_time,
    transaction_type                            as transaction_type,
    amount                                      as amount,
    balance_after                               as balance_after,
    merchant_name                               as merchant_name,
    merchant_category                           as merchant_category,
    channel                                     as channel,
    location                                    as location,
    description                                 as description,
    status                                      as status
from {{ ref('transactions') }}
