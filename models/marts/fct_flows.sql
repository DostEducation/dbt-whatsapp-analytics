with flow_results as (select * from {{ ref('int_flow_results') }}),
    flows as (select * from {{ ref('int_flows') }}),
    messages as (select * from {{ ref('fct_messages') }}),

    add_row_number_for_flow_results as (
        select
            *,
            row_number() over(partition by contact_phone, flow_uuid order by inserted_at desc) as row_number
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
    
    inbound_metrics as (
        select
            flows.flow_uuid,
            flows.flow_name,
            count(if(start_node_in_flow = 'Yes', contact_phone,null)) as flows_opted_in,
            count(if(final_node_in_flow = 'Yes', contact_phone,null)) as flows_completed
        from flows
        left join select_latest_response_for_flow_result using (flow_uuid)
        group by 1,2
    ),
    outbound_metrics as (
        select
            flows.flow_uuid,
            flows.flow_name,
            count(if(start_node_in_flow='Yes', contact_phone,null)) as flows_started
        from flows
        left join select_latest_response_for_messages using (flow_uuid)
        group by 1,2
    ),
    join_metrics as (
        select
            flows.*except(flow_config_json),
            flows_opted_in,
            flows_completed,
            flows_started
        from flows
        left join inbound_metrics using (flow_uuid)
        left join outbound_metrics using (flow_uuid)
    )

select
    *
from join_metrics
