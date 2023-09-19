with flow_results as (select * from {{ ref('int_flow_results') }}),
    messages as (select * from {{ ref('fct_messages') }}),

    add_row_number_for_flow_results as (
        select
            *,
            row_number() over(partition by contact_phone, flow_name order by inserted_at desc) as row_number
        from flow_results
        where inserted_at >= '2023-09-01'
    ),

    select_latest_response_for_flow_result as(
        select
            *
        from add_row_number_for_flow_results
        where row_number = 1
    ),

    add_row_number_for_messages as (
        select
            *,
            row_number() over(partition by contact_phone, node_label order by inserted_at desc) as row_number
        from messages
        where inserted_at >= '2023-09-01' and message_direction = 'outbound'
    ),

    select_latest_response_for_messages as(
        select
            *
        from add_row_number_for_messages
        where row_number = 1
    ),
    
    add_no_of_start_node as (
        select
            contact_phone,
            count(if(start_node_in_flow = 'Yes', contact_phone,null)) as no_of_start_flow_responded,
            count(if(final_node_in_flow = 'Yes', contact_phone,null)) as no_of_final_flow_responded,
            count(if(flow_success_node_in_flow = 'Yes' and desired_response = 'Yes', contact_phone, null)) as no_of_desired_response
        from select_latest_response_for_flow_result
        group by 1
    ),
    add_no_of_start_message_received as (
        select
            contact_phone,
            count(if(start_node_in_flow = 'Yes' and select_latest_response_for_messages.message_direction='outbound', contact_phone,null)) as no_start_message_received
        from select_latest_response_for_messages
        group by 1
    ),
    join_start_message_responded_receieved as (
        select
            add_no_of_start_message_received.*,
            no_of_start_flow_responded,
            no_of_final_flow_responded,
            no_of_desired_response
        from add_no_of_start_message_received
        left join add_no_of_start_node using (contact_phone)
        
    )
select
    *
from join_start_message_responded_receieved
