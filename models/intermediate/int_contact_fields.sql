with
    contacts as (select * from {{ ref("stg_contacts") }}),

    unnest_contact_fields as (
        select
            contacts.id as contact_id,
            lower(contact_fields.label) contact_field,
            lower(contact_fields.value) contact_field_value
        from contacts, unnest(contacts.fields) as contact_fields
        where array_length(contacts.fields) > 0
    ),

    aggregate_by_label as (
        select
            contact_field,
            contact_field_value,
            count(contact_id) as count_of_contacts
        from unnest_contact_fields
        where
            contact_field in (
                'Age Group',
                'Parent',
                'center',
                'District',
                'sector',
                'Education',
                'No of children',
                'Block',
                'system_phone',
                'Occupation',
                'Audio Sharing'
            )
        group by 1, 2
        order by 1, 2
    )

select *
from unnest_contact_fields
