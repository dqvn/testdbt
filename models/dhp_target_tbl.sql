{{ config (
materialized="table"
)}}

select 
    id as uid,
    status as data1,
    patient as data2
from Observation