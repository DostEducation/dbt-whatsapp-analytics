with daily_user_engagement as (select * from {{ ref('int_daily_user_engagement') }}),
    contacts as (select * from {{ ref('fct_contacts') }}),

    change_date_bool as (
        select
            contact_phone,
            date_day,
            flow_uuid_started,
            case when date_started is not null then 'Yes' else "No" end as flow_started,
            flow_uuid_opted_in,
            case when date_opted_in is not null then 'Yes' else "No" end as flow_opted_in,
            flow_uuid_completed,
            case when date_completed is not null then 'Yes' else "No" end as flow_completed,
            flow_uuid_succeeded,
            case when date_succeeded is not null then 'Yes' else "No" end as flow_succeeded
        from daily_user_engagement
    )

select
    change_date_bool.*,
    parent_type,
    programme,
    city,
    experience_type,
    sign_up_status
from change_date_bool
left join contacts using (contact_phone)

