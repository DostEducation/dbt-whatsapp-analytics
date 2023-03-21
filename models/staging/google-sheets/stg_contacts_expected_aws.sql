with source as (

    select * from {{ source('whatsapp_analytics_config', 'src_contacts_expected_aws') }}

),

renamed as (

    select
        id as google_sheet_id,
        name,
        concat('91', phone) as contact_phone,
        block,
        sector,
        district,
        reporting_to,
        _airbyte_ab_id,
        _airbyte_emitted_at,
        _airbyte_normalized_at,
        _airbyte_src_contacts_expected_aws_hashid,
        'AWS' as user_type_from_google_sheets

    from source

)

select * from renamed
