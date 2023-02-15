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
where
    true
    and flow_uuid in (
        'bf2e5555-689f-4708-b9b5-cc6bab8ecf70', -- activation flow
        '094ed199-1b6c-42a6-80bb-f46617fbb937', -- aws registration 1.0
        '9c797785-3062-4295-a824-c3237ecbc98a', -- aws registration 1.1
        'a4900527-f7bc-4dd7-afea-32803280cde1' -- aws registration 1.2
    )