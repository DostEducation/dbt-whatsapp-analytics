with
    
    contacts as (select google_sheet_id, name, reporting_to, onboarding_status, user_type_from_google_sheets from {{ ref('int_contacts') }}),
    
    AWS_contacts as (select * from contacts where user_type_from_google_sheets = 'AWS'),

    AWW_contacts as (select * from contacts where user_type_from_google_sheets = 'AWW'),

    join_AWS_to_AWW as (
        select
            AWS_contacts.google_sheet_id,
            AWW_contacts.google_sheet_id as AWW_id,
            AWW_contacts.onboarding_status as AWW_onboarding_status,
        from AWS_contacts
            left join AWW_contacts on AWS_contacts.name = AWW_contacts.reporting_to
    ),

    total_AWWs_mapped as (
        select
                google_sheet_id,
                count(distinct AWW_id) as total_AWWs
            from join_AWS_to_AWW
            group by 1
    ),

    AWWs_by_onboarding_status as (
        select
            google_sheet_id,
            AWW_onboarding_status,
            count(distinct AWW_id) as count_of_AWW
        from join_AWS_to_AWW
        group by 1, 2
    ),

    pivot_by_onboarding_status as (
        select *
        from AWWs_by_onboarding_status
        pivot(sum(count_of_AWW) for AWW_onboarding_status in ('complete', 'pending', 'not_added_to_google_sheet'))
    ),
    
    percentage_not_onboarded as (
        select
            google_sheet_id,
            total_AWWs as total_AWWs_mapped,
            complete as AWWs_onboarding_complete,
            pending as AWWs_onboarding_pending,
            not_added_to_google_sheet as AWWs_not_added_to_google_sheet,
            safe_divide(pending, total_AWWs) as percent_AWWs_not_onboarded
        from
            pivot_by_onboarding_status
            left join total_AWWs_mapped using (google_sheet_id)
    )

select * from percentage_not_onboarded