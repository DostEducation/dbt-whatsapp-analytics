with
    contacts as (select * from {{ source("glific", "contacts") }}),

    get_latest_contact_record as (
        select *
        from
            (
                select
                    row_number() over (
                        partition by id order by bq_inserted_at desc
                    ) as row_number,
                    contacts.*
                from contacts
            )
        where row_number < 2
    ),

    select_and_rename_columns as (
        select
            id as contact_id,
            bq_uuid as contact_bq_uuid,
            name as contact_name,
            phone as contact_phone,
            provider_status,
            status as contact_status,
            language as contact_language,
            contact_optin_method,
            optin_time as contact_optin_time,
            optout_time as contact_optout_time,
            last_message_at as contact_last_message_at,
            inserted_at as contact_inserted_at,
            updated_at as contact_updated_at,
            user_name,
            user_role,
            -- fields,
            -- settings,
            -- groups,
            tags,
            -- raw_fields,
            -- group_labels,
            bq_inserted_at as contact_bq_inserted_at,
        from get_latest_contact_record
    )

select *
from select_and_rename_columns
