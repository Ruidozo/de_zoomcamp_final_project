with 

source as (

    select * from {{ source('staging', 'real_estate_data_weekly') }}

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

),
joined as (
    select 
        renamed.*,
        pt."Latitude" as latitude,
        pt."Longitude" as longitude
    from renamed
    left join {{ ref('pt') }} pt
    on LOWER(renamed.city) = LOWER(pt.municipio)
)

select * from joined