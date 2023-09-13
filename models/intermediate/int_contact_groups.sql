with
    contacts as (select * from {{ ref("stg_contacts") }}),

    unnest_contact_groups as (
        select
            contact_id,
            contact_phone,
            lower(contact_groups.label) as group_name
            {# contact_fields.inserted_at as contact_field_inserted_at #}
        from contacts
        left join unnest(contact_groups) as contact_groups
        where
            contact_groups is not null
    ),
    contact_ids as (select contact_id, contact_phone from unnest_contact_groups group by 1, 2),
    open_ended as (select contact_id, group_name as open_ended_experience from unnest_contact_groups where group_name = 'dd_open-ended_experience'),
    semi_guided as (select contact_id, group_name as semi_guided_experience from unnest_contact_groups where group_name = 'dd_semi-guided_experience'),
    guided as (select contact_id, group_name as guided_experience from unnest_contact_groups where group_name = 'dd_guided_experience'),
    joining_contact_with_id as (
        select  
            *
        from contact_ids
        left join open_ended using (contact_id)
        left join semi_guided using (contact_id)
        left join guided using (contact_id)
    )
select *
from joining_contact_with_id
