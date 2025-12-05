-- models/stagging/stg_customer.sql
{{ config(materialized='view') }}

select
    customer_id                                   as customer_id,
    first_name                                    as first_name,
    last_name                                     as last_name,
    first_name || ' ' || last_name               as customer_name,
    cast(date_of_birth as date)                  as date_of_birth,
    gender                                        as gender,
    email                                         as email,
    cast(phone as varchar)                       as phone,
    address                                       as address,
    city                                          as city,
    province                                      as province,
    cast(postal_code as varchar)                 as postal_code,
    occupation                                    as occupation,
    income_level                                  as income_level,
    risk_rating                                   as risk_rating,
    cast(registration_date as date)              as registration_date,
    cast(last_updated as date)                   as last_updated
from {{ ref('customers') }}
