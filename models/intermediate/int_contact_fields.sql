with
    contacts as (select * from {{ ref("stg_contacts") }}),

    unnest_contact_fields as (
        select
            contact_id,
            contact_phone,
            lower(contact_fields.label) contact_field,
            lower(contact_fields.value) contact_field_value,
            contact_fields.inserted_at as contact_field_inserted_at
        from contacts, unnest(contacts.fields) as contact_fields
        where array_length(contacts.fields) > 0
    ),

    add_row_number as (
        select
            *,
            row_number() over (
                partition by contact_phone, contact_field
                order by contact_field_inserted_at desc
            ) as row_number
        from unnest_contact_fields
    ),

    select_latest_inserted_row as (select * from add_row_number where row_number = 1),

    contact_ids as (select contact_id, contact_phone from select_latest_inserted_row group by 1, 2),

    user_type as (select contact_id, contact_field_value as user_type from select_latest_inserted_row where contact_field = 'user type'),
    district as (select contact_id, contact_field_value as district from select_latest_inserted_row where contact_field = 'district'),
    block as (select contact_id, contact_field_value as block from select_latest_inserted_row where contact_field = 'block'),
    sector as (select contact_id, contact_field_value as sector from select_latest_inserted_row where contact_field = 'sector'),
    last_flow as (select contact_id, contact_field_value as last_flow from select_latest_inserted_row where contact_field = 'last_flow'),
    name_of_awc as (select contact_id, contact_field_value as name_of_awc from select_latest_inserted_row where contact_field = 'name of awc'),
    number_of_awc as (select contact_id, contact_field_value as number_of_awc from select_latest_inserted_row where contact_field = 'no.of awc'),
    number_0_to_3_kids as (select contact_id, contact_field_value as number_0_to_3_kids from select_latest_inserted_row where contact_field = 'number of 0-3 kids'),
    number_3_to_6_kids as (select contact_id, contact_field_value as number_3_to_6_kids from select_latest_inserted_row where contact_field = 'number of 3-6 kids'),
    flow_execution_date as (select contact_id, contact_field_value as flow_execution_date from select_latest_inserted_row where contact_field = 'flow_execution_date'),
    user_department as (select contact_id, contact_field_value as user_department from select_latest_inserted_row where contact_field = 'user department'),
    programme as (select contact_id, contact_field_value as programme from select_latest_inserted_row where contact_field = 'dd_programme'),
    sign_up_status as (select contact_id, contact_field_value as sign_up_status from select_latest_inserted_row where contact_field = 'dd_sign-up-status'),
    parent_type as (select contact_id, contact_field_value as parent_type from select_latest_inserted_row where contact_field = 'parent_type'),
    city as (select contact_id, contact_field_value as city from select_latest_inserted_row where contact_field = 'dd_city'),

    join_contact_fields_with_id as (
        select *
        from
            contact_ids
            left join user_type using (contact_id)
            left join district using (contact_id)
            left join block using (contact_id)
            left join sector using (contact_id)
            left join last_flow using (contact_id)
            left join name_of_awc using (contact_id)
            left join number_of_awc using (contact_id)
            left join number_0_to_3_kids using (contact_id)
            left join number_3_to_6_kids using (contact_id)
            left join flow_execution_date using (contact_id)
            left join user_department using (contact_id)
            left join programme using (contact_id)
            left join sign_up_status using (contact_id)
            left join parent_type using (contact_id)
            left join city using (contact_id)
    )

select *
from join_contact_fields_with_id

