use role sysadmin;
use schema dev_db.clean_sch;
use warehouse adhoc_wh;


-- de-duplication of the records + flattening it
with cte as (
select id
,index_record_ts
,json_data
,record_count
,json_version
,_stg_file_name
,_stg_file_load_ts
,_stg_file_md5
,_copy_data_ts
from dev_db.stage_sch.raw_aqi
qualify row_number() over(partition by index_record_ts order by _stg_file_load_ts desc) = 1
)
select index_record_ts
,f.value:country::varchar as country
,f.value:state::varchar as state
,f.value:city::varchar as city
,f.value:station::varchar as station
,f.value:latitude::varchar as latitude
,f.value:longitude::varchar as longitude
,f.value:pollutant_id::varchar as pollutant_id
,f.value:pollutant_max::varchar as pollutant_max
,f.value:pollutant_min::varchar as pollutant_min
,f.value:pollutant_avg::varchar as pollutant_avg
,_stg_file_name
,_stg_file_load_ts
,_stg_file_md5
,_copy_data_ts
from cte t
,lateral flatten (input => t.json_data:records) f;



create or replace dynamic table clean_aqi_dt
target_lag = 'DOWNSTREAM'
warehouse = transform_wh
as
with cte as (
select id
,index_record_ts
,json_data
,record_count
,json_version
,_stg_file_name
,_stg_file_load_ts
,_stg_file_md5
,_copy_data_ts
from dev_db.stage_sch.raw_aqi
qualify row_number() over(partition by index_record_ts order by _stg_file_load_ts desc) = 1
)
select index_record_ts
,f.value:country::varchar as country
,f.value:state::varchar as state
,f.value:city::varchar as city
,f.value:station::varchar as station
,f.value:latitude::varchar as latitude
,f.value:longitude::varchar as longitude
,f.value:pollutant_id::varchar as pollutant_id
,f.value:max_value::varchar as pollutant_max
,f.value:min_value::varchar as pollutant_min
,f.value:avg_value::varchar as pollutant_avg
,_stg_file_name
,_stg_file_load_ts
,_stg_file_md5
,_copy_data_ts
from cte t
,lateral flatten (input => t.json_data:records) f;


select * from clean_aqi_dt limit 10
