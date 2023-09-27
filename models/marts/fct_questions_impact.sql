with
    questions as (select * from {{ ref("stg_node_configs") }}),
    contacts as (select * from {{ ref("fct_contacts") }}),
    inbound_responses as (select * from {{ ref("int_flow_results") }}),
    outputs as (select * from {{ ref("stg_outputs") }}),

    cross_join_contacts_questions as (
        select contacts.*, node_label, question_english, output_id
        from contacts
            cross join questions
        where question_english is not null
    ),
    get_output_name as (
        select cross_join_contacts_questions.*, output_name as question_type
        from cross_join_contacts_questions
            left join outputs using (output_id)
    ),
    add_row_number_for_flow_results as (
        select
            *,
            row_number() over (
                partition by contact_phone, flow_uuid, node_label
                order by flow_result_inserted_at desc
            ) as row_number
        from inbound_responses
        where flow_result_inserted_at >= '2023-09-01'
    ),
    select_latest_record_for_flow_result as (
        select
            contact_phone,
            node_label,
            result_input,
            desired_response,
            max(flow_result_inserted_at) as flow_result_inserted_at
        from add_row_number_for_flow_results
        where row_number = 1
        group by 1, 2, 3, 4
    ),
    get_response_to_question as (
        select
            get_output_name.*,
            result_input,
            desired_response
        from get_output_name
            left join select_latest_record_for_flow_result using (contact_phone, node_label)
        where output_id in ('32', '33', '34')           -- to filter knowledge, attitude, behavior outputs
            and result_input is not null
    )

select *
from get_response_to_question
