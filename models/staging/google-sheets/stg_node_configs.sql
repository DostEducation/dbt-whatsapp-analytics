with source as (

    select * from {{ source('whatsapp_analytics_config', 'src_nodes') }}

),

renamed as (

    select
        sr_no as node_serial_number,
        flow_uuid,
        output_id,
        flow_name,
        node_label,
        node_type,
        -- __REF_output,
        -- outcome_type,
        question_english,
        final_node_in_flow,
        -- _airbyte_ab_id,
        -- _airbyte_emitted_at,
        -- _airbyte_normalized_at,
        -- _airbyte_src_nodes_hashid,
    from source

)

select * from renamed
