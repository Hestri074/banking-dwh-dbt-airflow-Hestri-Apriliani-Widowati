-- models/intermediate/int_daily_balance.sql
{{ config(materialized='view') }}

with ordered_tx as (
    select
        t.account_id,
        t.transaction_date,
        t.balance_after,
        row_number() over (
            partition by t.account_id, t.transaction_date
            order by t.transaction_time desc, t.transaction_id desc
        ) as rn
    from {{ ref('stg_transaction') }} as t
)

select
    account_id           as account_id,
    transaction_date     as balance_date,
    balance_after        as end_of_day_balance
from ordered_tx
where rn = 1
