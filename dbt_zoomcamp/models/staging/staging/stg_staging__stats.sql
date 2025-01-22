with 
  source as (
      select * from {{ source('staging', 'stats') }}
  ),
  renamed as (
      select
          _column_name as column_name,
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
  ),
  anomaly_counts as (
      select
          'total_area' as column_name,
          count(*) as anomaly_count
      from {{ ref('stg_staging__real_estate_data') }}
      where total_area <= 10
      union all
      select
          'price' as column_name,
          count(*) as anomaly_count
      from {{ ref('stg_staging__real_estate_data') }}
      where price <= 100
      union all
      select
          'price_per_sqm' as column_name,
          count(*) as anomaly_count
      from {{ ref('stg_staging__real_estate_data') }}
      where price / nullif(total_area, 0) not between 100 and 50000
  ),
  anomaly_percentages as (
      select
          a.column_name,
          a.anomaly_count,
          a.anomaly_count * 1.0 / (select count(*) from {{ ref('stg_staging__real_estate_data') }}) as anomaly_percentage
      from anomaly_counts a
  )
select 
    renamed.*,
    coalesce(anomaly_percentages.anomaly_count, 0) as anomaly_count,
    coalesce(anomaly_percentages.anomaly_percentage, 0) as anomaly_percentage
from renamed
left join anomaly_percentages 
on renamed.column_name = anomaly_percentages.column_name
