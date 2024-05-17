with source as (select * from {{ source("whatsapp_webhook", "users") }})

select *
from source
