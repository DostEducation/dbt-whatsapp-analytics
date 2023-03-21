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
    and flow_uuid in (
        'bf2e5555-689f-4708-b9b5-cc6bab8ecf70', -- activation flow
        '094ed199-1b6c-42a6-80bb-f46617fbb937', -- aws registration 1.0
        '9c797785-3062-4295-a824-c3237ecbc98a', -- aws registration 1.1
        'a4900527-f7bc-4dd7-afea-32803280cde1' -- aws registration 1.2
    )