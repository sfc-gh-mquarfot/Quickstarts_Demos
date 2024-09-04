--Create compute cluster for DocAI to isolate consumption from other workloads -----------------------
create or replace warehouse doc_ai_wh
    warehouse_size = 'medium'
    auto_suspend = 60
    auto_resume = true
    initially_suspended = true;

--Create database and schema for DocAI work -------------------------------------------------------
create database if not exists doc_ai_db;
create schema if not exists doc_ai_db.doc_ai_schema;

--Create DocAI role and grant database role to it -------------------------------------------------
use role accountadmin;
create role if not exists doc_ai_role;

grant database role snowflake.document_intelligence_creator to role doc_ai_role;

--Grant proper usage permissions ------------------------------------------------------------------
grant usage, operate on warehouse doc_ai_wh to role doc_ai_role;
grant usage on database doc_ai_db to role doc_ai_role;
grant usage on schema doc_ai_db.doc_ai_schema to role doc_ai_role;

--Grant create and execute task permissions to doc_ai_role ----------------------------------------
grant create stage on schema doc_ai_db.doc_ai_schema to role doc_ai_role;

grant create snowflake.ml.document_intelligence on schema doc_ai_db.doc_ai_schema to role doc_ai_role;

grant
    create stream,
    create table,
    create task,
    create view on schema doc_ai_db.doc_ai_schema to role doc_ai_role;

grant execute task on account to role doc_ai_role;

--Grant role to my user ----------------------------------------------------------------------------

grant role doc_ai_role to user mquarfot;


--------------------------------------------------------------------------------------------------

--Go to AI/ML and create new Document AI model:
    -- inspection_date: What is the inspection date?
    -- inspection_grade: What is the grade?
    -- inspector: Who performed the inspection?
    -- list_of_units: What are all the units?

--------------------------------------------------------------------------------------------------

--Set context for object reference and creation --------------------------------------------------

use role doc_ai_role;
use warehouse doc_ai_wh;
use database doc_ai_db;
use schema doc_ai_db.doc_ai_schema;

--Create document processing pipeline ------------------------------------------------------------

create stage if not exists my_pdf_stage
    directory = (enable = true)
    encryption = (type = 'snowflake_sse');

create stream my_pdf_stream on stage my_pdf_stage;

alter stage my_pdf_stage refresh;

use database doc_ai_db;
use schema doc_ai_db.doc_ai_schema;


--Create pdf_reviews table ------------------------------------------------------------------------

create or replace table pdf_reviews (
  file_name VARCHAR,
  file_size VARIANT,
  last_modified VARCHAR,
  snowflake_file_url VARCHAR,
  json_content VARCHAR
);


--Create task to load new files uploaded to my_pdf_stage -------------------------------------------

CREATE OR REPLACE TASK load_new_file_data
  WAREHOUSE = doc_ai_wh
  SCHEDULE = '1 minute'
  COMMENT = 'Process new files in the stage and insert data into the pdf_reviews table.'
WHEN SYSTEM$STREAM_HAS_DATA('my_pdf_stream')
AS
INSERT INTO pdf_reviews (
  SELECT
    RELATIVE_PATH AS file_name,
    size AS file_size,
    last_modified,
    file_url AS snowflake_file_url,
    inspection_reviews!PREDICT(GET_PRESIGNED_URL('@my_pdf_stage', RELATIVE_PATH), 1) AS json_content
  FROM my_pdf_stream
  WHERE METADATA$ACTION = 'INSERT'
);

alter task load_new_file_data resume;


--View information from new documents and create flattened table -------------------------------------

select * from pdf_reviews;

CREATE OR REPLACE TABLE doc_ai_db.doc_ai_schema.pdf_reviews_2 AS (
 WITH temp AS (
   SELECT
     RELATIVE_PATH AS file_name,
     size AS file_size,
     last_modified,
     file_url AS snowflake_file_url,
     inspection_reviews!PREDICT(get_presigned_url('@my_pdf_stage', RELATIVE_PATH), 1) AS json_content
   FROM directory(@my_pdf_stage)
 )

 SELECT
   file_name,
   file_size,
   last_modified,
   snowflake_file_url,
   json_content:__documentMetadata.ocrScore::FLOAT AS ocrScore,
   f.value:score::FLOAT AS inspection_date_score,
   f.value:value::STRING AS inspection_date_value,
   g.value:score::FLOAT AS inspection_grade_score,
   g.value:value::STRING AS inspection_grade_value,
   i.value:score::FLOAT AS inspector_score,
   i.value:value::STRING AS inspector_value,
   ARRAY_TO_STRING(ARRAY_AGG(j.value:value::STRING), ', ') AS list_of_units
 FROM temp,
   LATERAL FLATTEN(INPUT => json_content:inspection_date) f,
   LATERAL FLATTEN(INPUT => json_content:inspection_grade) g,
   LATERAL FLATTEN(INPUT => json_content:inspector) i,
   LATERAL FLATTEN(INPUT => json_content:list_of_units) j
 GROUP BY ALL
);

SELECT * FROM pdf_reviews_2;


---------------------------------------------------------------------------------------------
--Tutorial Reset Script

drop task load_new_file_data;
drop table pdf_reviews;
drop table pdf_reviews_2;
drop stream my_pdf_stream;
drop stage my_pdf_stage;
drop schema doc_ai_db.doc_ai_schema;
drop database doc_ai_db;
drop warehouse doc_ai_wh;
drop role doc_ai_role;