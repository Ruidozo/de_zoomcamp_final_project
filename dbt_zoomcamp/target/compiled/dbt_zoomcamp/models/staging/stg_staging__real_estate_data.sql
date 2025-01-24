with 

source as (
    select * from "mage_db"."public"."real_estate_data"
),

renamed as (
    select
        unique_id,
        price,
        district,
        city,  -- Ensure this matches the CSV column
        town,  -- Matches the real estate data table
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
),

joined as (
    select 
        renamed.*,
        towns.latitude,
        towns.longitude
    from renamed
    left join "mage_db"."public_public"."pt" towns
    on renamed.city = towns.city -- Update this to match your CSV structure
)

select * from joined