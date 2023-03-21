with
    messages as (select * from {{ ref('stg_messages') }})

select * from messages