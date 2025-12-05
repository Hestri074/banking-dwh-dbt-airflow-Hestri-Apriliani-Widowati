-- models/marts/dim/dim_customer.sql
{{ config(materialized='view') }}

select
    customer_id                  as customer_key, -- surrogate key
    customer_id                  as customer_id,
    first_name                   as first_name,
    last_name                    as last_name,
    customer_name                as customer_name,
    gender                       as gender,
    date_of_birth                as date_of_birth,
    email                        as email,
    phone                        as phone,
    address                      as address,
    city                         as city,
    province                     as province,
    postal_code                  as postal_code,
    occupation                   as occupation,
    income_level                 as income_level,
    risk_rating                  as risk_rating,
    registration_date            as registration_date,
    last_updated                 as last_updated
from {{ ref('stg_customer') }}
