with raw as (select * from {{source("edtech_source","enrollments")}})
select *
from raw