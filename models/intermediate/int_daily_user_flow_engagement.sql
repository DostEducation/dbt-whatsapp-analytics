with contacts as (Select * from {{ ref('fct_contacts') }}),
    flows as (Select * from {{ ref('int_flows') }}),
    inbound_metrics as (Select * from {{ ref('int_flow_results') }}),
    outbound_metrics as (select * from {{ ref('fct_messages') }}),

    cross_join_the_two as (
        select
            contacts.contact_phone,
            flows.flow_uuid
        from contacts
        cross join flows
    ),
    add_row_number_for_flow_results as (
        select
            *,
            row_number() over (
                partition by contact_phone, flow_uuid order by flow_result_inserted_at desc
            ) as row_number
        from inbound_metrics
        where flow_result_inserted_at >= '2023-09-01'
    ),

    select_latest_record_for_flow_result as (
        select * from add_row_number_for_flow_results where row_number = 1
    ),
    add_row_number_for_messages as (
        select
            *,
            row_number() over (
                partition by contact_phone, flow_uuid order by message_inserted_at desc
            ) as row_number
        from outbound_metrics
        where message_inserted_at >= '2023-09-01' and message_direction = 'outbound' and node_type = 'HSM Mesage'
    ),

    select_latest_record_for_messages as (
        select * from add_row_number_for_messages where row_number = 1
    ),
    calculate_date_started as (
        select
            contact_phone,
            flow_uuid,
            cast(message_inserted_at as date) as date_started
        from select_latest_record_for_messages
        where start_node_in_flow = 'Yes'
    ),
    calculate_date_opted_in as (
        select
            contact_phone,
            flow_uuid,
            cast(flow_result_inserted_at as date) as date_opted_in
        from select_latest_record_for_flow_result
        where start_node_in_flow = 'Yes'
    ),
    calculate_date_completed as (
        select
            contact_phone,
            flow_uuid,
            cast(flow_result_inserted_at as date) as date_completed
        from select_latest_record_for_flow_result
        where final_node_in_flow = 'Yes'
    ),
    calculate_date_succeeded as (
        select
            contact_phone,
            flow_uuid,
            cast(flow_result_inserted_at as date) as date_succeeded
        from select_latest_record_for_flow_result
        where flow_success_node_in_flow = 'Yes'
    )

select
    *
from cross_join_the_two
left join calculate_date_started using (contact_phone, flow_uuid)
left join calculate_date_opted_in using (contact_phone, flow_uuid)
left join calculate_date_completed using (contact_phone, flow_uuid)
left join calculate_date_succeeded using (contact_phone, flow_uuid)
