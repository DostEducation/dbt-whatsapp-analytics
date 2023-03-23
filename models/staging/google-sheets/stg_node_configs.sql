with source as (

    select * from {{ source('whatsapp_analytics_config', 'src_nodes') }}

),

renamed as (

    select
        sr_no as node_serial_number,
        node_label,
        question_english,
        intended_outcome,
        flow_name,
        flow_uuid,
        final_node_in_flow,
        -- _airbyte_ab_id,
        -- _airbyte_emitted_at,
        -- _airbyte_normalized_at,
        -- _airbyte_src_nodes_hashid

    from source

)

select * from renamed
