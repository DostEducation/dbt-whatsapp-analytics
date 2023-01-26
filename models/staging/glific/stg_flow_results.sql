with
    flow_results as (select * from {{ source("glific", "flow_results") }}),

    add_row_number as (
        select
            *,
            row_number() over (
                partition by uuid order by updated_at desc
            ) as row_number
        from flow_results
    ),

    get_latest_flow_results as (
        select * from add_row_number
        where row_number = 1
        order by uuid
    ),

    rename_columns as (
        select
            uuid as flow_result_uuid,
            inserted_at as flow_result_inserted_at,
            updated_at as flow_result_updated_at,
            results,
            flow_version,
            contact_phone,
            flow_context_id,
            bq_inserted_at as flow_result_bq_inserted_at,
            profile_id
        from get_latest_flow_results
    )

select * from rename_columns