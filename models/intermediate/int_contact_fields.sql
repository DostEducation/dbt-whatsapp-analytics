with
    contacts as (select * from {{ ref("stg_contacts") }}),

    unnest_contact_fields as (
        select
            contact_id,
            contact_phone,
            lower(contact_fields.label) contact_field,
            lower(contact_fields.value) contact_field_value,
            contact_fields.inserted_at as contact_field_inserted_at
        from contacts, unnest(contacts.fields) as contact_fields
        where array_length(contacts.fields) > 0
    ),

    add_row_number as (
        select
            *,
            row_number() over (partition by contact_phone, contact_field order by contact_field_inserted_at desc) as row_number
        from unnest_contact_fields
    ),

    select_latest_inserted_row as (
        select * from add_row_number where row_number = 1
    )

select *
from select_latest_inserted_row

