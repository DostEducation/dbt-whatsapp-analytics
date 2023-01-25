with
    src as (select * from {{ source("glific", "flows") }}),

    add_max_bq_inserted_at as (
        select
            *,
            row_number() over (
                partition by uuid order by bq_inserted_at desc
            ) as row_number
        from src
    ),

    filter_latest_version_of_flow as (
        select * from add_max_bq_inserted_at where row_number = 1
    ),

    select_and_rename_columns as (
        select
            id as flow_id,
            uuid as flow_uuid,
            name as flow_name,
            status as flow_status,
            inserted_at,
            updated_at,
            revision as flow_config_json,
            keywords,
        from filter_latest_version_of_flow
    )

select *
from select_and_rename_columns
