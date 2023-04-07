with source as (

    select * from {{ source('whatsapp_analytics_config', 'src_flows') }}

),

renamed as (

    select
        sr_no as flow_sr_no,
        name as flow_name,
        user_for,
        flow_uuid,
        objective as flow_objective,
        -- outcome as flow_outcome,
        sequence_number as flow_sequence_number,
        day as flow_day,
        -- _airbyte_ab_id,
        -- _airbyte_emitted_at,
        -- _airbyte_normalized_at,
        -- _airbyte_src_flows_hashid
    from source

)

select * from renamed
