with
    flow_results as (select * from {{ ref('int_flow_results') }}),

    aggregate_responses_by_outcome as (
        select
            flow_outcome,
            count(distinct contact_phone) as total_users_responding
        from flow_results
        group by 1
    ),

    aggregate_responses_by_outcome_and_response as(
        select
            flow_outcome,
            result_input,
            count(distinct contact_phone) as count_of_contacts
        from flow_results
        group by 1, 2
    )

-- select
--     result_key,
--     result_category,
--     total_responses,
--     count_of_contacts,
--     safe_divide(count_of_contacts, total_responses) as response_rate
-- from
--     aggregate_responses_by_result_key 
--     left join aggregate_responses_by_result_key_and_category using (result_key)
-- order by
--     3 desc, 1, 5 desc

select * from aggregate_responses_by_outcome_and_response