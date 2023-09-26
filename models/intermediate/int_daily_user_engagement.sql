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
        select contact_phone,date_started, count(distinct (flow_uuid)) as flows_started,
        from user_flow
        where date_started is not null
        group by 1,2
    ),
    join_contact_date_opted_in as (
        select contact_phone,date_opted_in, count(distinct (flow_uuid)) as flows_opted_in,
        from user_flow
        where date_opted_in is not null
        group by 1,2
    ),
    join_contact_date_completed as (
        select contact_phone,date_completed, count(distinct (flow_uuid)) as flows_completed,
        from user_flow
        where date_completed is not null
        group by 1,2
    ),
    join_contact_date_success as (
        select contact_phone,date_succeeded, count(distinct (flow_uuid)) as flows_succeeded
        from user_flow
        where date_succeeded is not null
        group by 1,2
    ),
    collate_all_dates as (
        select join_contact_date.*, flows_started, flows_opted_in, flows_completed, flows_succeeded
        from join_contact_date
        left join join_contact_date_started on join_contact_date_started.contact_phone = join_contact_date.contact_phone
                                            and join_contact_date_started.date_started = join_contact_date.date_day
        left join join_contact_date_opted_in on join_contact_date_opted_in.contact_phone = join_contact_date.contact_phone
                                            and join_contact_date_opted_in.date_opted_in = join_contact_date.date_day
        left join join_contact_date_completed on join_contact_date_completed.contact_phone = join_contact_date.contact_phone
                                            and join_contact_date_completed.date_completed = join_contact_date.date_day
        left join join_contact_date_success on join_contact_date_success.contact_phone = join_contact_date.contact_phone
                                            and join_contact_date_success.date_succeeded = join_contact_date.date_day
    )

select *
from collate_all_dates
order by flows_started desc
