with flow_results as (select * from {{ ref("int_user_onboarding") }})

select *
from flow_results
