with
    contact_groups as (select * from {{ ref('int_contact_groups') }}),

    group_by_collection as (
        select
            group_name as collection,
            count(distinct contact_id) as count_of_contacts
        from
            contact_groups
        group by 1
        order by 2 desc
    )

select * from group_by_collection