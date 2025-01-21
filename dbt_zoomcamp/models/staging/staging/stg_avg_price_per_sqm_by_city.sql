with
  base as (
      select
          city,
          district,
          total_area,
          price,
          price / nullif(total_area, 0) as price_per_sqm
      from "mage_db"."public"."real_estate_data"
      where total_area > 0 and price > 0
  )
select
  city,
  district,
  count(*) as property_count,
  avg(price_per_sqm) as avg_price_per_sqm
from base
group by city, district
order by avg_price_per_sqm desc
