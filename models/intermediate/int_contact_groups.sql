with
    contacts as (select * from {{ ref("stg_contacts") }}),

    unnest_contact_groups as (
        select
            contact_id,
            contact_groups.label as group_name
        from contacts
        left join unnest(contact_groups) as contact_groups
        where
            contact_groups is not null
            and contact_phone in ('919819352801', '919321578978')
    )

select *
from unnest_contact_groups
