{{ config(materialized='view') }}

with tripdata_2019 as 
(
  select *,
    row_number() over(partition by '"VendorID"', 'lpep_pickup_datetime') as rn
  from {{ source('raw','green_tripdata_2019') }}
  where '"VendorID"' is not null 
),

tripdata_2020 as 
(
  select *,
    row_number() over(partition by '"VendorID"', 'lpep_pickup_datetime') as rn
  from {{ source('raw','green_tripdata_2020') }}
  where '"VendorID"' is not null 
),

tripdata as 
(
  select * from tripdata_2019
  union all
  select * from tripdata_2020
)

select
    -- identifiers
    {{ dbt_utils.surrogate_key(['"VendorID"', 'lpep_pickup_datetime']) }} as tripid,
    "VendorID"::integer as vendorid,
    "RatecodeID"::integer as ratecodeid,
    "PULocationID"::integer as pickup_locationid,
    "DOLocationID"::integer as dropoff_locationid,

    -- timestamps
    lpep_pickup_datetime::timestamp as pickup_datetime,
    lpep_dropoff_datetime::timestamp as dropoff_datetime,

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

-- -- dbt build --m <model.sql> --var 'is_test_run: false'
-- {% if var('is_test_run', default=true) %}

--   limit 100

-- {% endif %}