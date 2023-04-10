with
    outputs as (select * from {{ ref("stg_outputs") }}),

    contacts as (select * from {{ ref('fct_contacts') }}),

    flow_results as (select * from {{ ref('int_flow_results') }}),

    onboarded_users_by_type as (
        select
            user_type,
            count(distinct contact_phone) as total_users_on_glific
        from contacts
        where
            onboarding_status != 'pending'
            and user_type in ('AWW', 'AWS')
        group by 1
    ),

    onboarded_users_common as (
        select
            'common' as user_type,
            count(distinct contact_phone) as total_users_on_glific
        from contacts
        where
            onboarding_status != 'pending'
            and user_type in ('AWW', 'AWS')
        group by 1
    ),

    union_table as (
        select * from onboarded_users_by_type
        union all
        select * from onboarded_users_common
    ),

    add_total_users_on_glific as (
        select *
        from
            outputs
            left join union_table on user_type = user_for
    ),

    users_responding as (
        select
            output_id,
            count(distinct contact_phone) as users_responding,
            count(distinct if (desired_response = 'Yes', contact_phone, null)) as users_giving_desired_responses
        from flow_results
        where user_type in ('AWW', 'AWS')
        group by 1
    ),


    add_users_responding as (
        select *
        from
            add_total_users_on_glific
            left join users_responding using (output_id)
    ),

    calculate_percentages as (
        select
            *,
            safe_divide(users_responding, total_users_on_glific) as percent_users_responding,
            safe_divide(users_giving_desired_responses, total_users_on_glific) as percent_users_giving_desired_responses
        from add_users_responding
    )


select * from calculate_percentages
