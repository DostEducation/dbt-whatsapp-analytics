with
    contacts as (select * from {{ ref("fct_contacts") }}),
    user_flow as (select * from {{ ref("int_daily_user_flow_engagement") }}),
    date_spine as (
        {{
            dbt_utils.date_spine(
                datepart="day",
                start_date="cast('2023-09-01' as date)",
                end_date="current_date()",
            )
        }}
    ),
    join_contact_date as (
        select contact_phone, cast(date_spine.date_day as date) as date_day
        from contacts
        cross join date_spine
    ),
    join_contact_date_started as (
        select join_contact_date.*, flow_uuid as flow_uuid_started, date_started
        from join_contact_date
        left join
            user_flow
            on join_contact_date.contact_phone = user_flow.contact_phone
            and user_flow.date_started = join_contact_date.date_day
    ),
    join_contact_date_opted_in as (
        select join_contact_date.*, flow_uuid as flow_uuid_opted_in, date_opted_in
        from join_contact_date
        left join
            user_flow
            on join_contact_date.contact_phone = user_flow.contact_phone
            and user_flow.date_opted_in = join_contact_date.date_day
    ),
    join_contact_date_completed as (
        select join_contact_date.*, flow_uuid as flow_uuid_completed, date_completed
        from join_contact_date
        left join
            user_flow
            on join_contact_date.contact_phone = user_flow.contact_phone
            and user_flow.date_completed = join_contact_date.date_day
    ),
    join_contact_date_success as (
        select join_contact_date.*, flow_uuid as flow_uuid_succeeded, date_succeeded
        from join_contact_date
        left join
            user_flow
            on join_contact_date.contact_phone = user_flow.contact_phone
            and user_flow.date_succeeded = join_contact_date.date_day
    ),
    collate_all_dates as (
        select *
        from join_contact_date
        left join join_contact_date_started using (contact_phone, date_day)
        left join join_contact_date_opted_in using (contact_phone, date_day)
        left join join_contact_date_completed using (contact_phone, date_day)
        left join join_contact_date_success using (contact_phone, date_day)
    )

select *
from collate_all_dates
    
