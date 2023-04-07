with
    source as (

        select * from {{ source("whatsapp_analytics_config", "src_geographies") }}

    ),

    renamed as (

        select
            sr_no,
            state,
            district,
            block,
            sector,
            sector_hindi,
            -- _airbyte_ab_id,
            -- _airbyte_emitted_at,
            -- _airbyte_normalized_at,
            -- _airbyte_src_geographies_hashid,
        from source

    )

select *
from renamed