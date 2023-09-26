with
    daily_user_engagement as (select * from {{ ref('int_daily_user_engagement') }}),
    contacts as (select * from {{ ref('fct_contacts') }}),
    messages as (select * from {{ ref('fct_messages') }}),
    
    find_user_registration_date as (
        select
            contact_phone,
            max(cast(message_inserted_at as date)) as registration_date
        from messages
        where message_inserted_at >= '2023-09-01' and node_label = 'pf01n01'
        group by 1
    ),

    get_flow_interaction_statuses as (
        select
            contact_phone,
            date_day,
            flow_uuid_started,
            case when date_started is not null then 'Yes' else "No"
            end as flow_started,
            flow_uuid_opted_in,
            case
                when date_opted_in is not null then 'Yes' else "No"
            end as flow_opted_in,
            flow_uuid_completed,
            case
                when date_completed is not null then 'Yes' else "No"
            end as flow_completed,
            flow_uuid_succeeded,
            case
                when date_succeeded is not null then 'Yes' else "No"
            end as flow_succeeded
        from daily_user_engagement
    ),

    get_all_contact_details as (
        select
            *
        from get_flow_interaction_statuses
            left join contacts using (contact_phone)
            left join find_user_registration_date using (contact_phone)
    )

select *
from get_all_contact_details