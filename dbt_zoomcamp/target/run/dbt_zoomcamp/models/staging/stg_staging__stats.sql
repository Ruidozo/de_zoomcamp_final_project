
  create view "mage_db"."public"."stg_staging__stats__dbt_tmp"
    
    
  as (
    with 

source as (

    select * from "mage_db"."public"."stats"

),

renamed as (

    select
        _column_name,
        data_type,
        missing_percent,
        missing_quant,
        dups,
        uniques,
        _count,
        _min,
        _max,
        average,
        standard_deviation

    from source

)

select * from renamed
  );