with source as (

    select * from {{ source('whatsapp_analytics_config', 'src_flows') }}

),

renamed as (

    select
        sr_no,
        name,
        user_for,
        flow_uuid,
        sequence_number,
        day,
        _airbyte_ab_id,
        _airbyte_emitted_at,
        _airbyte_normalized_at,
        _airbyte_src_flows_hashid

    from source

)

select * from renamed
