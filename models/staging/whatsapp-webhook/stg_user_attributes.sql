with
    source as (select * from {{ source("whatsapp_webhook_prod", "user_attributes") }}),

    user_attributes as (

        select id, user_id, user_phone, field_name, field_value from source

    )

select *
from user_attributes
