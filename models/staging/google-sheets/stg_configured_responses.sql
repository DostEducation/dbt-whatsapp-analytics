with
    source as (

        select * from {{ source("whatsapp_analytics_config", "src_configured_responses") }}

    ),

    renamed as (

        select
            id as configured_response_id,
            -- flow_name,
            -- node_type,
            node_label,
            result_input,
            result_category,
            -- question_english,
            desired_response_ as desired_response,
            -- __LOOKUP_question_english,
            -- _airbyte_ab_id,
            -- _airbyte_emitted_at,
            -- _airbyte_normalized_at,
            -- _airbyte_src_outputs,
        from source

    )

select *
from renamed