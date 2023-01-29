with
    flow_results as (select * from {{ ref('int_extract_flow_results') }}),

    aggregate_responses_by_result_key as (
        select
            result_key,
            count(distinct contact_phone) as count_of_contacts
        from flow_results
        group by 1
    ),

    aggregate_responses_by_result_key_and_category as(
        select
            result_key,
            result_category,
            count(distinct contact_phone) as count_of_contacts
        from flow_results
        group by 1, 2
        order by 1, 2
    )