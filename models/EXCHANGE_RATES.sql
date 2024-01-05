{{ config (
materialized="table"
)}}
           
select
    'QUANG' as subject,
    category,
    patient,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    NOW() as _AIRBYTE_NORMALIZED_AT
from Observation
