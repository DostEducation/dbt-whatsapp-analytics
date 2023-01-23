with
    contacts as (select * from {{ ref("stg_contacts") }}),

    unnest_contact_groups as (
        select
            contacts.id as contact_id,
            contact_groups.label as group_name
        from contacts
        left join unnest(contacts.groups) as contact_groups
        where array_length(contacts.groups) > 0
    )

select *
from unnest_contact_groups
