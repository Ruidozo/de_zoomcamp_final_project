with 
  base as (
      select *
      from {{ ref('stg_staging__real_estate_data') }}
  )
select *
from base
where total_area > 10
  and price > 100
  and price / nullif(total_area, 0) between 100 and 50000;
