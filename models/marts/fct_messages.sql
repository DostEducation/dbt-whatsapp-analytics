with
    messages as (select * from {{ ref('int_messages') }})

select *except(flow_config_json) from messages