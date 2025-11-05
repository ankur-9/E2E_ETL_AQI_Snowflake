use role sysadmin;
use warehouse adhoc_wh;
use schema dev_db.stage_sch;

create or replace file format json_file_format 
      type = 'JSON'
      compression = 'AUTO' 
      comment = 'this is json file format object';

create or replace stage raw_stg
directory = ( enable = true)
comment = 'all the air quality raw data will store in this internal stage location';

show stages;
list @raw_stg;
remove @raw_stg

select 
try_to_timestamp(t.$1:records[0].last_update::varchar,'dd-mm-yyyy hh24:mi:ss') as index_record_ts
,t.$1
,t.$1:total::int as record_count
,t.$1:version::varchar as json_version
,metadata$filename as _stg_file_name
,metadata$FILE_LAST_MODIFIED as _stg_file_load_ts
,metadata$FILE_CONTENT_KEY as _stg_file_md5
,current_timestamp as copy_data_ts
from @dev_db.stage_sch.raw_stg (file_format => json_file_format) t



create or replace transient table raw_aqi
(
    id int primary key autoincrement,
    index_record_ts timestamp not null,
    json_data variant not null,
    record_count number not null default 0,
    json_version varchar not null,
    _stg_file_name varchar,
    _stg_file_load_ts timestamp,
    _stg_file_md5 varchar,
    _copy_data_ts timestamp default current_timestamp()
)



create or replace task copy_air_quality_data
    warehouse = load_wh
    schedule = 'USING CRON 0 * * * * Asia/Kolkata'

as
copy into raw_aqi (index_record_ts,json_data,record_count,json_version,_stg_file_name,_stg_file_load_ts,_stg_file_md5,_copy_data_ts) 
from 
(
    select 
        Try_TO_TIMESTAMP(t.$1:records[0].last_update::text, 'dd-mm-yyyy hh24:mi:ss') as index_record_ts,
        t.$1,
        t.$1:total::int as record_count,
        t.$1:version::text as json_version,
        metadata$filename as _stg_file_name,
        metadata$FILE_LAST_MODIFIED as _stg_file_load_ts,
        metadata$FILE_CONTENT_KEY as _stg_file_md5,
        current_timestamp() as _copy_data_ts
            
   from @dev_db.stage_sch.raw_stg as t
)
file_format = (format_name = 'dev_db.stage_sch.JSON_FILE_FORMAT') 
ON_ERROR = ABORT_STATEMENT;


use role accountadmin;
grant execute task, execute managed task on account to role sysadmin;
use role sysadmin;

alter task dev_db.stage_sch.copy_air_quality_data resume;

--truncate table raw_aqi
select * from raw_aqi
