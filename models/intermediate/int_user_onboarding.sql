with
    glific_users as (select * from {{ ref('stg_users') }}),
    
    glific_users_attributes as (select * from {{ ref('stg_user_attributes') }}),
    
    user_onboarding_data as (
        select
            user_id,
            max(case when field_name = 'district' then field_value else null end) as district,
            max(case when field_name = 'state' then field_value else null end) as state,
            max(case when field_name = 'child_count' then field_value else null end) as child_count,
            max(case when field_name = 'parent_type' then field_value else null end) as parent_type
        from
            glific_users_attributes
        group by
            user_id
    ),

    join_tables as (
        select
            glific_users.*,
            user_onboarding_data.* except (user_id)
        from
            glific_users
            left join user_onboarding_data using (user_id)
    )

select * from join_tables
where user_id is not null
