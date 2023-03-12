{{ config(materialized='view') }}

with tripdata as 
(
  select *,
    row_number() over(partition by vendorid, tpep_pickup_datetime) as rn
  from {{ source('raw','yellow_tripdata_2019') }}
  where vendorid is not null 
)

select
    -- identifiers
    {{ dbt_utils.surrogate_key(['vendorid', 'tpep_pickup_datetime']) }} as tripid,
    vendorid::integer,
    ratecodeid::integer,
    pulocationid::integer as pickup_locationid,
    dolocationid::integer as dropoff_locationid,

    -- timestamps
    tpep_pickup_datetime::timestamp as pickup_datetime,
    tpep_dropoff_datetime::timestamp as dropoff_datetime,

    -- trip info
    store_and_fwd_flag,
    passenger_count::integer,
    trip_distance::numeric,
    -- yellow cabs are always street-hail
    1::integer as trip_type,

    -- payment info
    fare_amount::numeric,
    extra::numeric,
    mta_tax::numeric,
    tip_amount::numeric,
    tolls_amount::numeric,
    0::integer as ehail_fee,
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