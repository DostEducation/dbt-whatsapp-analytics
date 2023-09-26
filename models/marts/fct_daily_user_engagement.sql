with daily_user_engagement as (select * from {{ ref('int_daily_user_engagement') }}),
    contacts as (select * from {{ ref('fct_contacts') }}),
    messages as (select * from {{ ref('fct_messages') }}),
    
    add_row_number_for_messages as (
        select
            *,
            row_number() over (
                partition by contact_phone, flow_uuid order by message_inserted_at desc
            ) as row_number
        from messages
        where message_inserted_at >= '2023-09-01' and node_label = 'pf01n01'
    ),
    select_latest_record_for_messages as (
        select * from add_row_number_for_messages where row_number = 1
    ),
    
    find_user_registration_date as(
        select
            contact_phone,
            flow_uuid,
            cast(message_inserted_at as date) as registration_date
        from select_latest_record_for_messages
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
            get_flow_interaction_statuses.*,
        from get_flow_interaction_statuses
            left join contacts using (contact_phone)
    ),

    get_user_registration_date as (
        select
            get_all_contact_details.*,
            registration_date
        from get_all_contact_details
            left join find_user_registration_date
                on find_user_registration_date.contact_phone = get_all_contact_details.contact_phone
                                            and find_user_registration_date.registration_date = get_all_contact_details.date_day
    )

select *
from get_user_registration_date