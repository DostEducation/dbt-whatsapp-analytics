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

    -- only for reference
    aggregate_by_label as (
        select
            contact_field,
            contact_field_value,
            count(contact_id) as count_of_contacts
        from unnest_contact_fields
        -- where
        --     contact_field in (
        --         'qge group',
        --         'parent',
        --         'center',
        --         'district',
        --         'sector',
        --         'education',
        --         'no of children',
        --         'block',
        --         'system_phone',
        --         'occupation',
        --         'audio aharing'
        --     )
        group by 1, 2
        order by 1, 2
    )

select *
from unnest_contact_fields
