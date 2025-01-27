
  create view "mage_db"."public"."stg_staging__real_estate_data__dbt_tmp"
    
    
  as (
    with 

source as (

    select * from "mage_db"."public"."real_estate_data"

),

renamed as (

    select
        unique_id,
        price,
        district,
        city,
        town,
        _type,
        energy_certificate,
        gross_area,
        total_area,
        parking,
        has_parking,
        _floor,
        construction_year,
        energy_efficiency_level,
        publish_date,
        garage,
        elevator,
        electric_cars_charging,
        total_rooms,
        number_of_bedrooms,
        number_of_w_c,
        conservation_status,
        living_area,
        lot_size,
        built_area,
        number_of_bathrooms,
        push_date

    from source

)

select * from renamed
  );