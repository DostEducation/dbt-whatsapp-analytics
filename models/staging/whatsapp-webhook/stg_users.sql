with
    source as (

        select * from {{ source("whatsapp_webhook_prod", "users") }}

    ),

    users as (

        select
            id as user_id,
            name as user_name,
            phone as user_phone,
            location as user_location
        from source

    )

select *
from users
