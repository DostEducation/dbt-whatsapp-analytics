-- One row per continuation offer: the bot asks whether the caregiver wants to
-- carry on, and this records what they said.
--
-- Worth its own model because it unblocks the same-day continuation metric,
-- which was unmeasurable while the offer was made but the answer never kept.

with
    as_json as (select * from {{ ref("stg_voicebot_events") }}),

    followups as (
        select * from as_json where json_value(payload, '$.event') = 'followup_answer'
    ),

    get_latest_per_call as (
        select *
        from
            (
                select
                    row_number() over (
                        partition by json_value(payload, '$.call_sid')
                        order by logged_at desc, insert_id desc
                    ) as row_number,
                    followups.*
                from followups
            )
        where row_number = 1
    ),

    select_and_rename_columns as (
        select
            json_value(payload, '$.call_sid') as call_sid,
            logged_at,
            json_value(payload, '$.hashed_phone_prefix') as hashed_phone_prefix,

            -- Whether the offer was actually made. The denominator: a caller
            -- who was never asked is not a caller who declined.
            cast(json_value(payload, '$.asked') as bool) as was_offer_made,

            -- Three values, not two: haan, nahi, and unclear. Kept as the enum
            -- it is, because collapsing unclear into either yes or no would
            -- invent a decision the caregiver never made -- it means the bot
            -- could not tell what was said, which is a different finding.
            json_value(payload, '$.answer') as answer,
            json_value(payload, '$.answer') = 'haan' as did_accept,
            json_value(payload, '$.answer') = 'nahi' as did_decline,
            json_value(payload, '$.answer') = 'unclear' as was_answer_unclear,

            json_value(payload, '$.suggestion_type') as suggestion_type
        from get_latest_per_call
    )

select *
from select_and_rename_columns
