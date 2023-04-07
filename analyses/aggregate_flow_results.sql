with
    flow_results as (select * from {{ ref('int_flow_results') }}),

    aggregate_responses_by_result_key as (
        select
            result_key,
            count(distinct contact_phone) as total_responses
        from flow_results
        group by 1
    ),

    aggregate_responses_by_result_key_and_category as(
        select
            result_key,
            flow_name,
            node_type_n1,
            question_english,
            result_category,
            result_input,
            count(distinct contact_phone) as count_of_contacts
        from flow_results
        group by 1, 2, 3, 4, 5, 6
    )

select
    result_key as node_label,
    flow_name,
    question_english,
    node_type_n1 as node_type,
    result_category,
    result_input,
    total_responses,
    count_of_contacts,
    safe_divide(count_of_contacts, total_responses) as response_rate
from
    aggregate_responses_by_result_key 
    left join aggregate_responses_by_result_key_and_category using (result_key)
where question_english is not null
order by
    2, 3, 4

