with
    outputs as (select * from {{ ref('stg_outputs') }}),
    contacts as (select * from {{ ref('fct_contacts') }}),

    contact_output_combinations as (
        select
            output_id,
            output_name,
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

    get_output_count as (
        select
            output_id,
            contact_phone,
            -- desired_response,
            count(if(desired_response = 'Yes', contact_phone, null)) as output_count
        from
            flow_results
        where
            output_id is not null
            and contact_phone is not null
            and desired_response is not null 
        group by 1, 2
    ),

    output_achievement as (
        select
            *,
            if(output_count >= 1, 'Yes', null) as output_demonstrated
        from
            contact_output_combinations
            left join get_output_count using (output_id, contact_phone) 
        -- where output_count is null
    ),

    demography_fields as (
        select
            output_achievement.*,
            contacts.* except(contact_phone, user_type)
        from
            output_achievement
            left join contacts using (contact_phone)
    )

{{ dbt_utils.deduplicate(
    relation='demography_fields',
    partition_by='output_id, contact_phone',
    order_by='_airbyte_emitted_at desc',
   )
}}