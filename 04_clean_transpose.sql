use role sysadmin;
use schema dev_db.clean_sch;
use warehouse adhoc_wh;


-- transpose the data from rows to columns.
-- create temp table air_quality_tmp as
create or replace dynamic table clean_flatten_aqi_dt
    target_lag='DOWNSTREAM'
    warehouse=transform_wh
as
with grouped_cte as (
select index_record_ts,country,state,city,station,latitude,longitude
,max(case when pollutant_id = 'PM10' then pollutant_avg end) as pm10_avg
,max(case when pollutant_id = 'PM2.5' then pollutant_avg end) as pm25_avg
,max(case when pollutant_id = 'SO2' then pollutant_avg end) as so2_avg
,max(case when pollutant_id = 'NO2' then pollutant_avg end) as no2_avg
,max(case when pollutant_id = 'NH3' then pollutant_avg end) as nh3_avg
,max(case when pollutant_id = 'CO' then pollutant_avg end) as co_avg
,max(case when pollutant_id = 'OZONE' then pollutant_avg end) as o3_avg
from clean_aqi_dt
group by all
)
select index_record_ts,country,state,city,station,latitude,longitude
,case when pm10_avg = 'NA' then 0
    when pm10_avg is null then 0
    else round(pm10_avg)
    end as pm10_avg

,case when pm25_avg = 'NA' then 0
    when pm25_avg is null then 0
    else round(pm25_avg)
    end as pm25_avg

,case when so2_avg = 'NA' then 0
    when so2_avg is null then 0
    else round(so2_avg)
    end as so2_avg

,case when no2_avg = 'NA' then 0
    when no2_avg is null then 0
    else round(no2_avg)
    end as no2_avg

,case when nh3_avg = 'NA' then 0
    when nh3_avg is null then 0
    else round(nh3_avg)
    end as nh3_avg

,case when co_avg = 'NA' then 0
    when co_avg is null then 0
    else round(co_avg)
    end as co_avg

,case when o3_avg = 'NA' then 0
    when o3_avg is null then 0
    else round(o3_avg)
    end as o3_avg
from grouped_cte
