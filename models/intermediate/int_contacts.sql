with
    glific_contacts as (select * from {{ ref("stg_contacts") }}),

    aww_contacts_expected as (select * from {{ ref("stg_contacts_expected_aww") }}),

    aws_contacts_expected as (select * from {{ ref("stg_contacts_expected_aws") }}),

    user_type as (
        select * from {{ ref("int_contact_fields") }} where contact_field = 'user type'
    ),

    append_aws_and_aww_contacts_expected as (
        select *
        from aws_contacts_expected
        union all
        select *
        from aww_contacts_expected
    ),

    join_expected_and_actual_contacts as (
        select
            *,
            case
                when contact_id is not null and google_sheet_id is not null
                then 'complete'
                when contact_id is null
                then 'pending'
                when google_sheet_id is null
                then 'not_added_to_google_sheet'
            end as onboarding_status,
        from append_aws_and_aww_contacts_expected
        full outer join glific_contacts using (contact_phone)
    ),

    add_user_type as (
        select
            join_expected_and_actual_contacts.*,
            upper(user_type.contact_field_value) as user_type_from_glific,
            user_type.contact_field_inserted_at
        from join_expected_and_actual_contacts
        left join user_type using (contact_id)
    )

select *
from add_user_type
