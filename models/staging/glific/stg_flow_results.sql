with
    flow_results as (select * from {{ source("glific", "flow_results") }}),

    add_row_number as (
        select
            *,
            row_number() over (
                partition by id order by bq_inserted_at desc
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
            id as flow_result_id,
            bq_uuid as flow_result_bq_uuid,
            uuid as flow_uuid,
            inserted_at as flow_result_inserted_at,
            updated_at as flow_result_updated_at,
            results,
            flow_version,
            contact_phone,
            flow_context_id,
            bq_inserted_at as flow_result_bq_inserted_at,
            profile_id, row_number
        from get_latest_flow_results
    )

select * from rename_columns
where
    true
    -- and contact_phone = '919819352801'
    and flow_uuid = '75640ab7-992c-40b8-9113-df8c4bdf2a65'
order by flow_result_id