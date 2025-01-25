select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      SELECT *
FROM "mage_db"."public"."stg_cleaned_real_estate_data" c
LEFT JOIN "mage_db"."public_public"."pt" p
ON LOWER(TRIM(c.town)) = LOWER(TRIM(p.municipio))
   OR LOWER(TRIM(c.city)) = LOWER(TRIM(p.municipio))
   OR LOWER(TRIM(c.district)) = LOWER(TRIM(p.municipio))
WHERE p.municipio IS NULL;
      
    ) dbt_internal_test