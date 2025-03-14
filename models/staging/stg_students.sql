with raw as (
select * from {{ source("edtech_source", 'students') }}
),
final as (
select 
r.*,
datediff(year,date_of_birth::date,sysdate()::date) as age,
year(enrollment_date) enrollment_year,
first_name||' '||last_name as full_name
from 
raw r
)
select *
from final