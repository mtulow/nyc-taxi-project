{{ config(materialized='view') }}

with tripdata as 
(
  select *,
    row_number() over(partition by vendorid, pickup_datetime) as rn
  from {{ source('raw','green_tripdata_2019') }}
  where vendorid is not null 
)

select
    -- identifiers
    {{ dbt_utils.surrogate_key(['vendorid', 'pickup_datetime']) }} as tripid,
    vendorid::integer,
    ratecodeid::integer,
    pickup_locationid::integer,
    dropoff_locationid::integer,

    -- timestamps
    pickup_datetime::timestamp,
    dropoff_datetime::timestamp,

    -- trip info
    store_and_fwd_flag,
    passenger_count::integer,
    trip_distance::numeric,
    trip_type::integer,

    -- payment info
    fare_amount::numeric,
    extra::numeric,
    mta_tax::numeric,
    tip_amount::numeric,
    tolls_amount::numeric,
    ehail_fee::integer,
    improvement_surcharge::numeric,
    total_amount::numeric,
    payment_type::integer,
    {{ get_payment_type_description('payment_type') }} as payment_type_description,
    congestion_surcharge::numeric
from tripdata
where rn = 1

-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}