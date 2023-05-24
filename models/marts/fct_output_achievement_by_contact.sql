with
    outputs as (select * from {{ ref('stg_outputs') }} ),

    contacts as (select * from {{ ref('fct_contacts') }}),

    cross_join as (
        select
            output_id,
            contact_phone,
            user_type,
            user_for
        from
            outputs
            cross join contacts
        where
            user_type = user_for or user_for = 'common'
    ),

    flow_results as (select * from {{ ref('int_flow_results') }}),

    get_desired_response as (
        select
            output_id,
            contact_phone,
            desired_response,
            -- count(if(desired_response = 'Yes', contact_phone, null)) as output_count
        from
            flow_results
        where
            output_id is not null
            and contact_phone is not null
            -- and desired_response is not null 
        group by 1, 2, 3
    )



select
    *
from
    flow_results