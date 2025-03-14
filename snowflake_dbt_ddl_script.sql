select * from test_db.test_schema.emp;

select * from test_db.dbt_transformed_schema.emp_dim;

update emp set city='Mng' where id=103;


--create or replace api integration git_api_snowflake_dbt
--    api_provider = git_https_api
--    api_allowed_prefixes = ('https://github.com/Punithl91/snowflake-dbt')
--    enabled = true
--    allowed_authentication_secrets = all


CREATE OR REPLACE PROCEDURE earlyReturn()
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python')
HANDLER='async_handler'
EXECUTE AS CALLER
AS $$
def async_handler(session):
    async_job = session.sql("select current_date()").collect_nowait()
    df = async_job.to_df()
    try:
        result=df.collect()
        return result[0]
    except Exception as ex:
        return 'Error: (02000): Result for query <UUID> has expired'
$$;

call earlyReturn();


create or replace table pii_raw(
ID int,
name varchar(25),
age int,
email varchar(20),
phone varchar(20),
gender varchar(20)
);

insert into pii_raw (ID, name ,age ,email ,phone ,gender)
values (1,'Punith',33,'p@gmail.com','9743498788','Male'),
       (2,'shreya',25,'shr.com','89897','Female'),
       (3,'Ram', 1000,'ram.com','454545','X');

       select * from pii_raw;