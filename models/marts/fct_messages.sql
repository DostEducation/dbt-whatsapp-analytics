with
    messages as (select * from {{ ref('int_messages') }})

select * from messages