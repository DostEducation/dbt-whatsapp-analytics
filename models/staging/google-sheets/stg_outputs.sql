with
    source as (

        select * from {{ source("whatsapp_analytics_config", "src_outputs") }}

    ),

    renamed as (

        select
            id as output_id,
            outputs,
            user_for,
            -- _airbyte_ab_id,
            -- _airbyte_emitted_at,
            -- _airbyte_normalized_at,
            -- _airbyte_src_outputs,
        from source

    )

select *
from renamed