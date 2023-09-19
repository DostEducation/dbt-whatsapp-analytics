with flow_results as (select * from {{ ref('int_flow_results') }}),
    contacts as (select * from {{ ref('int_contacts') }}),
    messages as (select * from {{ ref('fct_messages') }}),

    add_row_number_for_flow_results as (
        select
            *,
            row_number() over(partition by contact_phone, node_label order by inserted_at desc) as row_number
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
            contacts.contact_phone,
            count(if(start_node_in_flow = 'Yes', contact_phone,null)) as no_of_start_flow_responded
        from contacts
        left join select_latest_response_for_flow_result using (contact_phone)
        group by 1
    ),
    add_no_of_start_message_received as (
        select
            contacts.contact_phone,
            count(if(start_node_in_flow='Yes' and message_direction='outbound', contact_phone,null)) as no_start_message_received
        from contacts
        left join select_latest_response_for_messages using (contact_phone)
        group by 1
    ),
    join_start_message_responded_receieved as (
        select
            contacts.*,
            no_of_start_flow_responded,
            no_start_message_received
        from contacts
        left join add_no_of_start_node using (contact_phone)
        left join add_no_of_start_message_received using (contact_phone)
    )
select
    *
from join_start_message_responded_receieved


