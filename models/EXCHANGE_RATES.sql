{{ config (
materialized="table"
)}}
           
with __dbt__cte__EXCHANGE_RATES_AB1 as (

-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: "WTTDEMO".PUBLIC._AIRBYTE_RAW_EXCHANGE_RATES
select
    to_varchar(get_path(parse_json(_airbyte_data), '"base"')) as BASE,
    to_varchar(get_path(parse_json(_airbyte_data), '"date"')) as DATE,
    
        get_path(parse_json(table_alias._airbyte_data), '"rates"')
     as RATES,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    convert_timezone('UTC', current_timestamp()) as _AIRBYTE_NORMALIZED_AT
from "WTTDEMO".PUBLIC._AIRBYTE_RAW_EXCHANGE_RATES as table_alias
-- EXCHANGE_RATES
where 1 = 1

),  __dbt__cte__EXCHANGE_RATES_AB2 as (

-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: __dbt__cte__EXCHANGE_RATES_AB1
select
    cast(BASE as 
    varchar
) as BASE,
    cast(DATE as 
    varchar
) as DATE,
    cast(RATES as 
    variant
) as RATES,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    convert_timezone('UTC', current_timestamp()) as _AIRBYTE_NORMALIZED_AT
from __dbt__cte__EXCHANGE_RATES_AB1
-- EXCHANGE_RATES
where 1 = 1

),  __dbt__cte__EXCHANGE_RATES_AB3 as (

-- SQL model to build a hash column based on the values of this record
-- depends_on: __dbt__cte__EXCHANGE_RATES_AB2
select
    md5(cast(coalesce(cast(BASE as 
    varchar
), '') || '-' || coalesce(cast(DATE as 
    varchar
), '') || '-' || coalesce(cast(RATES as 
    varchar
), '') as 
    varchar
)) as _AIRBYTE_EXCHANGE_RATES_HASHID,
    tmp.*
from __dbt__cte__EXCHANGE_RATES_AB2 tmp
-- EXCHANGE_RATES
where 1 = 1

)-- Final base SQL model
-- depends_on: __dbt__cte__EXCHANGE_RATES_AB3
select
    BASE,
    DATE,
    RATES,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    convert_timezone('UTC', current_timestamp()) as _AIRBYTE_NORMALIZED_AT,
    _AIRBYTE_EXCHANGE_RATES_HASHID
from __dbt__cte__EXCHANGE_RATES_AB3
