with
    flow_results as (select * from {{ ref("int_flow_results") }}),
    messages as (select * from {{ ref("fct_messages") }}),

    add_row_number_for_flow_results as (
        select
            *,
            row_number() over (
                partition by contact_phone, flow_uuid order by inserted_at desc
            ) as row_number
        from flow_results
        where inserted_at >= '2023-09-01'
    ),

    select_latest_record_for_flow_result as (
        select * from add_row_number_for_flow_results where row_number = 1
    ),

    add_row_number_for_messages as (
        select
            *,
            row_number() over (
                partition by contact_phone, node_label order by inserted_at desc
            ) as row_number
        from messages
        where inserted_at >= '2023-09-01' and message_direction = 'outbound'
    ),

    select_latest_record_for_messages as (
        select * from add_row_number_for_messages where row_number = 1
    ),

    inbound_metrics as (
        select
            contact_phone,
            count(
                distinct
                if(start_node_in_flow = 'Yes', flow_uuid, null)
            ) as flows_opted_in,
            count(
                distinct
                if(final_node_in_flow = 'Yes', flow_uuid, null)
            ) as flows_completed,
            count(
                distinct
                if(
                    flow_success_node_in_flow = 'Yes' and desired_response = 'Yes',
                    flow_uuid,
                    null
                )
            ) as flows_succeeded
        from select_latest_record_for_flow_result
        group by 1
    ),

    outbound_metrics as (
        select
            contact_phone,
            count(
                distinct
                if(
                    start_node_in_flow = 'Yes',
                    flow_uuid,
                    null
                )
            ) as flows_started
        from select_latest_record_for_messages
        group by 1
    ),

    join_metrics as (
        select
            *
        from outbound_metrics
        left join inbound_metrics using (contact_phone)
    )

select *
from join_metrics
