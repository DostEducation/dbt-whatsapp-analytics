with
    messages as (select * from {{ ref('int_messages') }}),
    contact_field as (select * from {{ ref('int_contact_fields') }})

select messages.*except(flow_config_json),programme, sign_up_status,parent_type,city from messages
left join contact_field using (contact_phone)