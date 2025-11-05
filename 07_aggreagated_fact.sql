select * from location_dim where city = 'Bengaluru'
select * from date_dim where measurement_time = '2024-03-04 11:00:00.000'

with cte as (
select avg(pm10_avg) as pm10_avg
,avg(pm25_avg) as pm25_avg
,avg(so2_avg) as so2_avg
,avg(no2_avg) as no2_avg
,avg(nh3_avg) as nh3_avg
,avg(co_avg) as co_avg
,avg(o3_avg) as o3_avg
from
dev_db.consumption_sch.aqi_fact
where location_fk in (select location_pk from dev_db.consumption_sch.location_dim where city = 'Bengaluru')
and date_fk - (select date_pk from date_dim where measurement_time = '2024-03-04 11:00:00.000')
)
select cte.*
,prominent_index(pm10_avg,pm25_avg,so2_avg,no2_avg,nh3_avg,co_avg,o3_avg) as prominent_pollutant
,case when three_sub_index_criteria(pm10_avg,pm25_avg,so2_avg,no2_avg,nh3_avg,co_avg,o3_avg)>2
then greatest(pm10_avg,pm25_avg,so2_avg,no2_avg,nh3_avg,co_avg,o3_avg)
else 0
end as aqi
from cte


create or replace dynamic table agg_city_fact_hour_level
target_lag='30 min'
    warehouse=transform_wh
as 
with step01_city_hr_level_data as (
select 
    d.measurement_time as measurement_time
    ,l.country as country
    ,l.state as state
    ,l.city as city
    ,round(avg(pm10_avg)) as pm10_avg
    ,round(avg(pm25_avg)) as pm25_avg
    ,round(avg(so2_avg)) as so2_avg
    ,round(avg(no2_avg)) as no2_avg
    ,round(avg(nh3_avg)) as nh3_avg
    ,round(avg(co_avg)) as co_avg
    ,round(avg(o3_avg)) as o3_avg
from dev_db.consumption_sch.aqi_fact a
join date_dim d on d.date_pk = a.date_fk
join location_dim l on l.location_pk = a.location_fk
group by all
)
select *
,prominent_index(pm10_avg,pm25_avg,so2_avg,no2_avg,nh3_avg,co_avg,o3_avg) as prominent_pollutant
,case when three_sub_index_criteria(pm10_avg,pm25_avg,so2_avg,no2_avg,nh3_avg,co_avg,o3_avg)>2
then greatest(pm10_avg,pm25_avg,so2_avg,no2_avg,nh3_avg,co_avg,o3_avg)
else 0
end as aqi
from step01_city_hr_level_data


select 
    * 
from agg_city_fact_hour_level 
where 
    city = 'Bengaluru' and 
    MEASUREMENT_TIME ='2024-03-04 11:00:00.000'
order by 
    country, state, city, measurement_time
limit 100;








create or replace dynamic table agg_city_fact_day_level
target_lag='30 min'
    warehouse=transform_wh
as 
with step01_city_day_level_data as (
select 
    date(measurement_time) as measurement_date
    ,country as country
    ,state as state
    ,city as city
    ,round(avg(pm10_avg)) as pm10_avg
    ,round(avg(pm25_avg)) as pm25_avg
    ,round(avg(so2_avg)) as so2_avg
    ,round(avg(no2_avg)) as no2_avg
    ,round(avg(nh3_avg)) as nh3_avg
    ,round(avg(co_avg)) as co_avg
    ,round(avg(o3_avg)) as o3_avg
from dev_db.consumption_sch.agg_city_fact_hour_level
group by all
)
select *
,prominent_index(pm10_avg,pm25_avg,so2_avg,no2_avg,nh3_avg,co_avg,o3_avg) as prominent_pollutant
,case when three_sub_index_criteria(pm10_avg,pm25_avg,so2_avg,no2_avg,nh3_avg,co_avg,o3_avg)>2
then greatest(pm10_avg,pm25_avg,so2_avg,no2_avg,nh3_avg,co_avg,o3_avg)
else 0
end as aqi
from step01_city_day_level_data

select 
    * 
from agg_city_fact_hour_level 
where 
    city = 'Bengaluru' 
    order by 1
