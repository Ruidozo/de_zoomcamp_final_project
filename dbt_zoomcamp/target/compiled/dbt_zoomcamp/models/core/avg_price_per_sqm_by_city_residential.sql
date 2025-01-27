

WITH 
  base AS (
      SELECT
          city,
          district,
          total_area,
          price,
          price / NULLIF(total_area, 0) AS price_per_sqm
      FROM "mage_db"."public"."stg_cleaned_real_estate_data"
      WHERE _type IN ('Apartment', 'House', 'Duplex', 'Studio', 'Other-Residential', 'Mansion')
        AND total_area > 0  -- Ensure total_area is valid
        AND price > 0       -- Ensure price is valid
  )
SELECT
  ROW_NUMBER() OVER (ORDER BY ROUND(AVG(price_per_sqm)::numeric, 2) DESC) AS index,
  city,
  COUNT(*) AS property_count,
  ROUND(AVG(price_per_sqm)::numeric, 2) AS avg_price_per_sqm
FROM base
WHERE price_per_sqm <= 10000  -- Exclude rows where price_per_sqm is greater than 10,000
GROUP BY city
ORDER BY avg_price_per_sqm DESC