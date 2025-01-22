with 
  base as (
      select
          city,
          district,
          total_area,
          price,
          price / nullif(total_area, 0) as price_per_sqm
      from {{ ref('stg_cleaned_real_estate_data') }}
      WHERE _type IN ('Apartment', 'House', 'Duplex', 'Studio', 'Other-Residential', 'Mansion')
        and total_area > 0  -- Ensure total_area is valid
        and price > 0       -- Ensure price is valid
  )
select
  ROW_NUMBER() OVER (ORDER BY ROUND(AVG(price_per_sqm)::numeric, 2) DESC) AS index,
  city,
  count(*) as property_count,
  avg(price_per_sqm) as avg_price_per_sqm
from base
group by city
ORDER BY avg_price_per_sqm DESC
