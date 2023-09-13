{{
    config(
        materialized='table'
    )
}}


with
    glific_contacts as (select * from {{ ref("stg_contacts") }}),
    aww_contacts_expected as (select * from {{ ref("stg_contacts_expected_aww") }}),
    aws_contacts_expected as (select * from {{ ref("stg_contacts_expected_aws") }}),
    geographies as (select * from {{ ref("stg_geographies") }}),
    contact_fields as (select * from {{ ref('int_contact_fields') }}),
    contact_group as (select * from {{ ref('int_contact_groups') }}),

    append_aws_and_aww_contacts_expected as (
        select * from aws_contacts_expected
        union all
        select * from aww_contacts_expected
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

    add_contact_fields as (
        select
            join_expected_and_actual_contacts.*,
            contact_fields.* except (contact_id, contact_phone, user_type, district, block, sector),
            upper(user_type) as user_type_from_glific,
            contact_fields.block as block_from_gliffic,
            (sector) as sector_from_glific,
        from join_expected_and_actual_contacts
            left join contact_fields using (contact_id)
    ),

    get_english_names_for_sector as (
        select
            add_contact_fields.* except (sector_from_glific,block_from_gliffic),
            block_from_gliffic as block_from_gliffic_hindi,
            geographies.block as block_from_gliffic,
            sector_from_glific as sector_from_glific_hindi,
            geographies.sector as sector_from_glific
        from
            add_contact_fields
            left join geographies on sector_from_glific = sector
    ),

    consolidate_contact_fields as (
        select
            *,
            if(user_type_from_glific is null, user_type_from_google_sheets, user_type_from_glific) as user_type,
            if(sector_from_glific is null, sector_from_google_sheets, sector_from_glific) as sector,
        from get_english_names_for_sector
    ),

    add_row_number as (
        select
            *,
            row_number() over (partition by contact_phone) as row_count
        from consolidate_contact_fields
    ),

    remove_duplicate_phone_numbers as  (
        select
            * except(row_count)
        from add_row_number
        where row_count = 1
    ),
    adding_contact_group as (
        select
            remove_duplicate_phone_numbers.*,
            open_ended_experience,
            semi_guided_experience,
            guided_experience
        from remove_duplicate_phone_numbers
        left join contact_group using (contact_phone)
    )

select * from adding_contact_group