

WITH cleaned_real_estate AS (
    SELECT *
    FROM "mage_db"."public"."stg_cleaned_real_estate_data"
),

with_price_per_sqm AS (
    SELECT
        cleaned_real_estate.*,
        CASE 
            WHEN total_area > 0 THEN ROUND(price / total_area)
            ELSE NULL 
        END AS price_per_sqm
    FROM cleaned_real_estate
)

SELECT *
FROM with_price_per_sqm