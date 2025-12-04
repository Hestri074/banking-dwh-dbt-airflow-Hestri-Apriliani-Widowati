-- models/marts/fact/fact_transaction.sql
{{ config(materialized='view') }}

with tx as (
    select *
    from {{ ref('stg_transaction') }}
),
acc as (
    select *
    from {{ ref('stg_account') }}
),
cust as (
    select *
    from {{ ref('dim_customer') }}
)

select
    tx.transaction_id                as transaction_key, -- surrogate key (bisa = natural key)
    tx.transaction_id                as transaction_id,
    tx.account_id                    as account_id,
    acc.customer_id                  as customer_id,
    cust.customer_key                as customer_key,
    tx.transaction_date              as transaction_date,
    tx.transaction_time              as transaction_time,
    tx.transaction_type              as transaction_type,
    tx.amount                        as amount,
    tx.balance_after                 as balance_after,
    tx.merchant_name                 as merchant_name,
    tx.merchant_category             as merchant_category,
    tx.channel                       as channel,
    tx.location                      as location,
    tx.description                   as description,
    tx.status                        as status
from tx
left join acc
    on tx.account_id = acc.account_id
left join cust
    on acc.customer_id = cust.customer_id
