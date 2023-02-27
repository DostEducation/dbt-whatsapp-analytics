with dbt_test__target as (

  select contact_phone as unique_field
  from `cryptic-gate-211900`.`whatsapp_analytics_staging`.`stg_contacts`
  where contact_phone is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1