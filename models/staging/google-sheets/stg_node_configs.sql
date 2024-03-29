with
    source as (select * from {{ source("whatsapp_analytics_config", "src_nodes") }}),

    renamed as (
        select
            sr_no as node_serial_number,
            node_label,
            -- flow_uuid,
            output_id,
            node_type,
            -- flow_name,
            -- __REF_output,
            -- outcome_type,
            response_type_expected,
            question_english,
            start_node_in_flow,
            final_node_in_flow,
            flow_success_node_in_flow,
            -- _airbyte_ab_id,
            -- _airbyte_emitted_at,
            -- _airbyte_normalized_at,
            -- _airbyte_src_nodes_hashid,
        from source
    ),

    -- remove node configs not mapped to question OR not final node in flow
    filter_node_configs as (
        select *
        from renamed
        where
            question_english is not null
            or final_node_in_flow is not null
    )

{{ dbt_utils.deduplicate(
    relation='filter_node_configs',
    partition_by='node_label',
    order_by='node_serial_number asc',
   )
}}
