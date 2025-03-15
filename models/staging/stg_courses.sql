with raw as (select * from {{source("edtech_source","courses")}}), 
     final as (select
             from raw)