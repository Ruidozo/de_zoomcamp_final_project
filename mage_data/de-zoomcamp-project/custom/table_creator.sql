CREATE TABLE IF NOT EXISTS public.stats (
    "Column Name" TEXT,
    "Data Type" TEXT,
    "Missing#" BIGINT,
    "Missing%" FLOAT,
    "Dups" BIGINT,
    "Uniques" BIGINT,
    "Count" BIGINT,
    "Min" FLOAT,
    "Max" FLOAT,
    "Average" FLOAT,
    "Standard Deviation" FLOAT
);

SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'public'
  AND table_name = 'stats';


SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'stats'
  AND table_schema = 'public';
