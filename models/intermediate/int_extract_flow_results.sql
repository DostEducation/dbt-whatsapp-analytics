with
    flow_results as (select * from {{ ref('stg_flow_results') }}),

    select_columns_and_extract_keys as (
        select
            flow_result_uuid,
            -- flow_version,
            -- contact_phone,
            -- flow_context_id,
            results,
            `cryptic-gate-211900.918800625442.jsonObjectKeys`(results) as json_key_string
        from flow_results
    ),

    unnest_keys as (
        select
            *
        from
            select_columns_and_extract_keys,
            unnest(json_key_string) as key_array
    ),

    extract_results as (
        select
            *,
            json_value_array(key_array) [offset(0)] as result_name,
            json_value_array(key_array) [offset(1)] as result_value,
        from unnest_keys
    )

select * from extract_results