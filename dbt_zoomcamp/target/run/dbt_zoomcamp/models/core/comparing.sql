
  create view "mage_db"."public"."comparing__dbt_tmp"
    
    
  as (
    SELECT *, price / NULLIF(total_area, 0) AS price_per_sqm
FROM stg_staging__real_estate_data
WHERE total_area > 10
  AND (price / NULLIF(total_area, 0) < 100 OR price / NULLIF(total_area, 0) > 50000);
  );