

with source as (

    select * from {{ source('whatsapp_analytics_config', 'src_contacts_expected_aww') }}

),

renamed as (

    select
        id as google_sheet_id,
        trim(name) as name,
        concat('91', phone) as contact_phone,
        block,
        sector as sector_from_google_sheets,
        district,
        reporting_to,
        _airbyte_ab_id,
        _airbyte_emitted_at,
        _airbyte_normalized_at,
        _airbyte_src_contacts_expected_aww_hashid,
        'AWW' as user_type_from_google_sheets,

    from source

)

select * from renamed