with students as (
    select * from {{ref("stg_students")}}
    ),
 enrollments as (
    select * from {{ref("stg_enrollments")}}
    ),
 courses as (
    select * from {{ref("stg_courses")}}
    ),
 final as (
    select 
            s.STUDENT_ID,
            s.FIRST_NAME,
            s.LAST_NAME,
            s.EMAIL,
            s.ENROLLMENT_DATE,
            s.DATE_OF_BIRTH,
            s.GENDER,
            s.AGE,
            s.ENROLLMENT_YEAR,
            s.FULL_NAME,
            c.COURSE_NAME,
            c.INSTRUCTOR_NAME,
            c.START_DATE,
            c.END_DATE,
            c.CREDITS,
            c.COUSE_LEVEL,
            c.COURSE_STATUS,
            c.COURSE_DURATION_DAYS
    from 
    students s
    left join enrollments e on s.student_id=e.student_id 
    left join courses c on e.course_id=c.course_id
 )
 select *
 from 
 final
