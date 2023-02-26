with
    glific_contacts as (select * from {{ ref('stg_contacts') }}),

    googlesheet_contacts as (select * from {{ ref('stg_contacts_expected') }})

select
    *,
    case
        when contact_id is not null and google_sheet_id is not null then 'complete'
        when contact_id is null then 'pending'
        when google_sheet_id is null then 'not added to google sheet'
    end as onboarding_status,
from
    googlesheet_contacts
    full outer join glific_contacts using (contact_phone)
