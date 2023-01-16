with
    src as (
    select * from {{source ('glific','contacts')}}
    )

select
    *
from
    src