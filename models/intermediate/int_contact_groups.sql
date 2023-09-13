with
    contacts as (select * from {{ ref("stg_contacts") }}),

    unnest_contact_groups as (
        select
            contact_id,
            contact_phone,
            lower(contact_groups.label) as group_name
        from contacts, unnest(contact_groups) as contact_groups
        where
            contact_groups is not null
    ),

    add_row_number as (
        select
            *,
            row_number() over (
                partition by contact_phone, group_name
            ) as row_number
        from unnest_contact_groups
    ),

    select_latest_inserted_row as (select * from add_row_number where row_number = 1),

    contact_ids as (select contact_id, contact_phone from select_latest_inserted_row group by 1, 2),
    open_ended as (select contact_id, TRUE as open_ended_experience from select_latest_inserted_row where group_name = 'dd_open-ended_experience'),
    semi_guided as (select contact_id, TRUE as semi_guided_experience from select_latest_inserted_row where group_name = 'dd_semi-guided_experience'),
    guided as (select contact_id, TRUE as guided_experience from select_latest_inserted_row where group_name = 'dd_guided_experience'),

    join_tables as (
        select  
            *
        from contact_ids
        left join open_ended using (contact_id)
        left join semi_guided using (contact_id)
        left join guided using (contact_id)
    ),

    get_experience_type as (
        select
            *,
            case
                when open_ended_experience then 'open_ended'
                when semi_guided_experience then 'semi_guided'
                when guided_experience then 'guided'
            end as experience_type
        from
            join_tables
    )

select *
from get_experience_type
