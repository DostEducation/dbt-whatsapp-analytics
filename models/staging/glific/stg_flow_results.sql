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
    )

select * from get_latest_flow_results