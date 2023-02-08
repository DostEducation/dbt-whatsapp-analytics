with
    flow_counts as (select * from {{ source("glific", "flow_counts") }}),

    get_latest_flow_counts as (
        select *
        from
            (
                select
                    row_number() over (
                        partition by id order by bq_inserted_at desc
                    ) as row_number,
                    flow_counts.*
                from flow_counts
            )
        where row_number < 2
    ),

    select_and_rename_columns as (
        select
            id as flow_count_id,
            bq_uuid as flow_count_uuid,
            type as flow_count_type,
            flow_uuid,
            source_uuid as source_node_uuid,
            destination_uuid as destination_node_uuid,
            count as flow_count,
        from get_latest_flow_counts
    )

select * from select_and_rename_columns
