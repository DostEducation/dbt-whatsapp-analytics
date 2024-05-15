with source as (select * from {{ source("whatsapp_webhook_prod", "users") }})

select *
from source
