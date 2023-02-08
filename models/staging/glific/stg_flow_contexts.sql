with
    flow_contexts as (select * from {{ source("glific", "flow_contexts") }}),

    add_row_number as (
        select
            *,
            row_number() over (
                partition by flow_uuid, contact_phone order by updated_at desc
            ) as row_number
        from flow_contexts
    )

select * from add_row_number
where
    true
    and row_number = 1
    and flow_uuid = '1576a314-37b7-422b-b609-269e1e547356'