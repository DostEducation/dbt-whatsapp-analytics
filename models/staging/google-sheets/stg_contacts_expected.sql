with source as (

    select * from {{ source('whatsapp_analytics_config', 'src_contacts_expected') }}

),

renamed as (

    select
        id,
        name,
        phone,
        sector,
        user_type,
        reporting_to,
        _airbyte_ab_id,
        _airbyte_emitted_at,
        _airbyte_normalized_at,
        _airbyte_src_contacts_expected_hashid

    from source

)

select * from renamed
