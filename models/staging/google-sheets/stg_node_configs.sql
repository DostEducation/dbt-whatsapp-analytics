with source as (

    select * from {{ source('whatsapp_analytics_config', 'src_nodes') }}

),

renamed as (

    select
        link,
        sr_no,
        node_type,
        flow_name,
        node_label,
        intended_outcome,
        final_node_in_flow,
        -- _airbyte_ab_id,
        -- _airbyte_emitted_at,
        -- _airbyte_normalized_at,
        -- _airbyte_src_nodes_hashid

    from source

)

select * from renamed
