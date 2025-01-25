WITH base AS (
    SELECT *,
           CASE 
               -- If _type is 'farm' or 'land', prioritize total_area
               WHEN LOWER(_type) IN ('farm', 'land') THEN total_area
               -- If total_area is large and gross_area is valid, use gross_area
               WHEN total_area > 1000 AND gross_area > 0 THEN gross_area
               -- Fallback to total_area
               ELSE total_area
           END AS effective_area,
           -- Calculate price_per_sqm for filtering
           price / NULLIF(
               CASE 
                   WHEN LOWER(_type) IN ('farm', 'land') THEN total_area
                   WHEN total_area > 1000 AND gross_area > 0 THEN gross_area
                   ELSE total_area
               END, 0
           ) AS price_per_sqm
    FROM "mage_db"."public"."stg_staging__real_estate_data"
)
SELECT *
FROM base
WHERE effective_area > 10
  AND (
      -- Relax price_per_sqm for general cases
      price_per_sqm BETWEEN 50 AND 100000
      OR LOWER(_type) IN ('farm', 'land') -- Skip price_per_sqm filter for farms/land
  )