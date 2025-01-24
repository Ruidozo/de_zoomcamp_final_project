WITH base AS (
    SELECT *
    FROM "mage_db"."public"."stg_staging__real_estate_data"
)
SELECT *
FROM base
WHERE total_area > 10
  AND price > 100
  AND price / NULLIF(total_area, 0) BETWEEN 100 AND 50000