
  
    

  create  table "mage_db"."public"."avg_price_per_sqm_by_city_overall__dbt_tmp"
  
  
    as
  
  (
    

with 
  base as (
      select
          city,
          district,
          total_area,
          price,
          price / nullif(total_area, 0) as price_per_sqm
      from "mage_db"."public"."stg_cleaned_real_estate_data"
  )
select
  city,
  district,
  count(*) as property_count,
  avg(price_per_sqm) as avg_price_per_sqm
from base
group by city, district
order by avg_price_per_sqm desc
  );
  