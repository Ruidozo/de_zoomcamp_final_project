{{ config(materialized='table') }}

WITH date_trends AS (
    SELECT
        COALESCE(
            EXTRACT(YEAR FROM publish_date::DATE),
            EXTRACT(YEAR FROM push_date::DATE)
        ) AS year,
        CAST(
            COALESCE(
                EXTRACT(MONTH FROM publish_date::DATE),
                EXTRACT(MONTH FROM push_date::DATE)
            ) AS INT
        ) AS month, -- Ensure month is an integer
        ROUND(AVG(price)::numeric) AS avg_price, -- Remove decimal points
        COUNT(*) AS total_entries -- Count total listings
    FROM {{ ref('stg_cleaned_real_estate_data') }}
    WHERE price > 0
    GROUP BY year, month
)
SELECT
    year,
    month,
    TO_CHAR(avg_price, 'FM999,999,999') AS avg_price, -- No decimals, apply thousands separator
    TO_CHAR(total_entries, 'FM999,999,999') AS total_entries -- Apply thousands separator
FROM date_trends
ORDER BY year, month


