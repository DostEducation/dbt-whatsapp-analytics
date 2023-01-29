with
    nodes as (select * from {{ ref("int_nodes") }})

select
    node_type_n1,
    count(node_type_n1) as count_of_configured_instances
from nodes
group by 1
order by 1