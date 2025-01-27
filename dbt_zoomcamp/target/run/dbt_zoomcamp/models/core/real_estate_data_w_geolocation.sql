
  
    

  create  table "mage_db"."public"."real_estate_data_w_geolocation__dbt_tmp"
  
  
    as
  
  (
    

with 

cleaned_real_estate as (
    select * 
    from "mage_db"."public"."stg_cleaned_real_estate_data"
),

geolocation_data as (
    select 
        municipio,
        "Latitude",
        "Longitude"
    from "mage_db"."public_public"."pt"
),

joined_with_geolocation as (
    select
        cleaned_real_estate.*,
        geolocation_data."Latitude" as latitude,
        geolocation_data."Longitude" as longitude
    from cleaned_real_estate
    left join geolocation_data
    on LOWER(cleaned_real_estate.city) = LOWER(geolocation_data.municipio)
)

select * 
from joined_with_geolocation
  );
  