use role sysadmin;
use schema dev_db.consumption_sch;
use warehouse adhoc_wh;

----------------------------------------DATE DIM-----------------------------------------
with step01_hr_data as (
select index_record_ts as measurement_time
,year(index_record_ts) as aqi_year
,month(index_record_ts) as aqi_month
,quarter(index_record_ts) as aqi_quarter
,day(index_record_ts) as aqi_day
,hour(index_record_ts) as aqi_hour
from dev_db.clean_sch.clean_flatten_aqi_dt
group by all
)
--for date dim primary key is index_record_ts, but cannot use sequance generator here.
-- since we are using dynamic table we will use hash value 

select hash(measurement_time) as date_pk
,*
from step01_hr_data


create or replace dynamic table date_dim
    target_lag = '30 minute'
    warehouse = transform_wh
as
with step01_hr_data as (
select index_record_ts as measurement_time
,year(index_record_ts) as aqi_year
,month(index_record_ts) as aqi_month
,quarter(index_record_ts) as aqi_quarter
,day(index_record_ts) as aqi_day
,hour(index_record_ts) as aqi_hour
from dev_db.clean_sch.clean_flatten_aqi_dt
group by all
)
--for date dim primary key is index_record_ts, but cannot use sequance generator here.
-- since we are using dynamic table we will use hash value 

select hash(measurement_time) as date_pk
,*
from step01_hr_data


----------------------------------------LOCATION DIM-----------------------------------------

create or replace dynamic table location_dim
    target_lag = '30 minute'
    warehouse = transform_wh
as
with unique_loc_data as (
select latitude,longitude,country
,state,city,station
from dev_db.clean_sch.clean_flatten_aqi_dt
group by all
)
--latitute and logitude will be unique for earch station/record
select hash(latitude,longitude) as location_pk
,*
from unique_loc_data
order by country,state,city,station



----------------------------------------AQI FACT-----------------------------------------

select 
hash(index_record_ts) as date_fk,
hash(latitude,longitude) as location_fk,
pm10_avg,
pm25_avg,
so2_avg,
no2_avg,
nh3_avg,
co_avg,
o3_avg,
prominent_index(pm25_avg,pm10_avg,so2_avg,no2_avg,nh3_avg,co_avg,o3_avg)as prominent_pollutant,
case when three_sub_index_criteria(pm25_avg,pm10_avg,so2_avg,no2_avg,nh3_avg,co_avg,o3_avg) > 2 
     then greatest (pm25_avg,pm10_avg,so2_avg,no2_avg,nh3_avg,co_avg,o3_avg)
     else 0
end as aqi
from dev_db.clean_sch.clean_flatten_aqi_dt

/* Checking data
-487202788685179472	-423106335530792353

select * from date_dim where date_pk = '-487202788685179472' -- 2024-03-01 07:00:00.000
select * from location_dim where location_pk = '-423106335530792353' --India	Bihar	Purnia	Mariam Nagar, Purnia - BSPCB
select * from aqi_final_wide_dt where station = 'Mariam Nagar, Purnia - BSPCB' and index_record_ts = '2024-03-01 07:00:00.000'
PM10_AVG	PM25_AVG	SO2_AVG	NO2_AVG	NH3_AVG	CO_AVG	O3_AVG	PROMINENT_POLLUTANT	AQI
103	96	18	13	1	57	23	PM10	103
*/

create or replace dynamic table aqi_fact
    target_lag = '30 minute'
    warehouse = transform_wh
as
select 
hash(index_record_ts,latitude,longitude) as aqi_pk,
hash(index_record_ts) as date_fk,
hash(latitude,longitude) as location_fk,
pm10_avg,
pm25_avg,
so2_avg,
no2_avg,
nh3_avg,
co_avg,
o3_avg,
prominent_index(pm25_avg,pm10_avg,so2_avg,no2_avg,nh3_avg,co_avg,o3_avg)as prominent_pollutant,
case when three_sub_index_criteria(pm25_avg,pm10_avg,so2_avg,no2_avg,nh3_avg,co_avg,o3_avg) > 2 
     then greatest (pm25_avg,pm10_avg,so2_avg,no2_avg,nh3_avg,co_avg,o3_avg)
     else 0
end as aqi
from dev_db.clean_sch.clean_flatten_aqi_dt
