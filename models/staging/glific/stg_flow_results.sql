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
        select * from add_row_number where row_number = 1 order by uuid
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
            profile_id,
        from get_latest_flow_results
    ),

    extract_result_array as (
        select
            *,
            `cryptic-gate-211900.918800625442.extractKeys`(results) as result_array,
        from rename_columns
    )

    --- move below CTEs to intermediate file
    -- unnest_result_jsons as (
    --     select
    --         *,
    --     from
    --         extract_result_array,
    --         unnest(result_array) as result_json
    -- ),

    -- extract_result_details as (
    --     select
    --         *,
    --         json_value(result_json, '$.result_key') as result_key,
    --         json_value(result_json, '$.category') as result_category,
    --         json_value(result_json, '$.input') as result_input,
    --         json_value(result_json, '$.inserted_at') as result_inserted_at,
    --         json_value(result_json, '$.intent') as result_intent,
    --         json_query(result_json, '$.interactive') as result_interactive,
    --     from
    --         unnest_result_jsons
    -- )

select
    *
from extract_result_array
-- where
--     true
--     -- and contact_phone = '919819352801'
--     and flow_uuid = '75640ab7-992c-40b8-9113-df8c4bdf2a65'
--     and flow_result_id = 5107197