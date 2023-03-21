with
    messages as (select * from {{ ref("stg_messages") }}),

    flows as (select * from {{ ref('stg_flows') }}),

    add_flow_name as (
        select
            messages.* except (flow_name),
            flows.flow_name
        from messages
        left join flows using (flow_uuid)
    ),

    aggregate_contacts_count_by_flow_and_month as (
        select
            flow_uuid,
            flow_name,
            date_trunc(message_inserted_at, week(monday)) as message_week,
            count(distinct contact_phone) as count_of_contacts
        from
            add_flow_name
        group by 1, 2, 3
    )
    
select
    *
from aggregate_contacts_count_by_flow_and_month
where message_week >= '2023-03-13'
order by message_week desc
