with
    source as (

        select * from {{ source("whatsapp_analytics_config", "src_collections") }}

    ),

    renamed as (

        select
            id,
            name,
            description,
            _airbyte_ab_id,
            _airbyte_emitted_at,
            _airbyte_normalized_at,
            _airbyte_src_collections_hashid

        from source

    )

select *
from renamed
