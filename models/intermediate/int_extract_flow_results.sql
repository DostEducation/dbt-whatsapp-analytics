with
    flow_results as (select * from {{ ref('stg_flow_results') }}),

    unnest_result_jsons as (
        select
            * except(result_array),
        from
            flow_results,
            unnest(result_array) as result_json
    ),

    extract_result_details as (
        select
            * except(result_json),
            json_value(result_json, '$.result_key') as result_key,
            json_value(result_json, '$.category') as result_category,
            json_value(result_json, '$.input') as result_input,
            json_value(result_json, '$.inserted_at') as result_inserted_at,
            json_value(result_json, '$.intent') as result_intent,
            json_query(result_json, '$.interactive') as result_interactive,
        from
            unnest_result_jsons
    )

select * from extract_result_details
where
    true
    -- and flow_result_id = 5107197
    and contact_phone in ('919819352801', '919321578978')
    and flow_result_bq_inserted_at >= '2023-02-15'