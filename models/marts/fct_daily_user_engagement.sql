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
            flows_started as no_of_flows_started,
            case when flows_started is not null then 'Yes' else "No"
            end as no_of_flows_started_bool,
            flows_opted_in as no_of_flows_opted_in,
            case
                when flows_opted_in is not null then 'Yes' else "No"
            end as no_of_flows_opted_in_bool,
            flows_completed as no_of_flows_completed,
            case
                when flows_completed is not null then 'Yes' else "No"
            end as no_of_flows_completed_bool,
            flows_succeeded as no_of_flows_succeeded,
            case
                when flows_succeeded is not null then 'Yes' else "No"
            end as no_of_flows_succeeded_bool
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