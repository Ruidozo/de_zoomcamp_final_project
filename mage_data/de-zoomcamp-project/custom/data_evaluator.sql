SELECT 
    'unique_id' AS column_name,
    COUNT(*) AS total_rows,
    COUNT(unique_id) AS non_null_count,
    COUNT(*) - COUNT(unique_id) AS null_count,
    (COUNT(*) - COUNT(unique_id))::FLOAT / COUNT(*) * 100 AS null_percentage
FROM public.real_estate_data

UNION ALL

SELECT 
    'price' AS column_name,
    COUNT(*) AS total_rows,
    COUNT(price) AS non_null_count,
    COUNT(*) - COUNT(price) AS null_count,
    (COUNT(*) - COUNT(price))::FLOAT / COUNT(*) * 100 AS null_percentage
FROM public.real_estate_data

UNION ALL

SELECT 
    'district' AS column_name,
    COUNT(*) AS total_rows,
    COUNT(district) AS non_null_count,
    COUNT(*) - COUNT(district) AS null_count,
    (COUNT(*) - COUNT(district))::FLOAT / COUNT(*) * 100 AS null_percentage
FROM public.real_estate_data

UNION ALL

SELECT 
    'energy_certificate' AS column_name,
    COUNT(*) AS total_rows,
    COUNT(energy_certificate) AS non_null_count,
    COUNT(*) - COUNT(energy_certificate) AS null_count,
    (COUNT(*) - COUNT(energy_certificate))::FLOAT / COUNT(*) * 100 AS null_percentage
FROM public.real_estate_data


ORDER BY null_count DESC;


-- Duplicate Rows
SELECT 
    COUNT(*) AS total_rows,
    COUNT(DISTINCT (unique_id, price, district, city, _type, total_area)) AS unique_rows,
    COUNT(*) - COUNT(DISTINCT (unique_id, price, district, city, _type, total_area)) AS duplicate_rows
FROM public.real_estate_data;


-- Column Statistics
SELECT 
    MIN(price::NUMERIC) AS min_price,
    MAX(price::NUMERIC) AS max_price,
    AVG(price::NUMERIC) AS avg_price,
    STDDEV(price::NUMERIC) AS stddev_price
FROM public.real_estate_data;

-- First Few Rows
SELECT *
FROM public.real_estate_data
LIMIT 3;
