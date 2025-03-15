with raw as (select * from {{source("edtech_source","courses")}}), 
     final as (select
              r.*,
              case when credits=4 then 'Advanced' else 'Beginer' end as couse_level,
              case when end_date<=sysdate()::date then 'INACTIVE' else 'ACTIVE' end as COURSE_STATUS,
              DATEDIFF(day,start_date,end_date) as course_duration_days
             from raw r)
    select *
    from final