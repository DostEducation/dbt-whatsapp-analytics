-- One row per call, from the summary the bot emits when the call ends.
--
-- Read as JSON rather than as STRUCT columns on purpose: the sink's payload
-- schema only gains a field once some event has emitted it, so csat_rating --
-- populated on a small minority of calls -- is not yet a column at all.
-- Selecting it directly fails the whole model; JSON_VALUE returns null.

with
    as_json as (select * from {{ ref("stg_voicebot_events") }}),

    call_summaries as (
        select * from as_json where json_value(payload, '$.event') = 'call_summary'
    ),

    -- The sink delivers at least once, and a retried call can summarise twice.
    -- Keep the newest summary per call so the model's grain is one row per call.
    get_latest_summary_per_call as (
        select *
        from
            (
                select
                    row_number() over (
                        partition by json_value(payload, '$.call_sid')
                        order by logged_at desc, insert_id desc
                    ) as row_number,
                    call_summaries.*
                from call_summaries
            )
        where row_number = 1
    ),

    select_and_rename_columns as (
        select
            json_value(payload, '$.call_sid') as call_sid,
            insert_id as log_insert_id,
            logged_at,

            -- When the call began, as the bot saw it. logged_at is when the
            -- summary was written, which is the moment the call ENDED.
            cast(
                json_value(payload, '$.started_at') as timestamp
            ) as call_started_at,
            cast(json_value(payload, '$.duration_s') as float64) as call_duration_s,
            cast(json_value(payload, '$.turns') as int64) as turn_count,

            -- The caregiver key. Eight characters of a salted hash -- enough to
            -- group a caller's calls together, not enough to identify them.
            -- The raw number is never in this pipeline.
            json_value(payload, '$.hashed_phone_prefix') as hashed_phone_prefix,

            json_value(payload, '$.final_state') as final_state,

            -- Where the caller was when the call ended. The single most useful
            -- field for drop-off: 'consent' means they never got past the
            -- opening, 'sawaal' means they left mid-question.
            json_value(payload, '$.phase_at_disconnect') as phase_at_disconnect,

            cast(json_value(payload, '$.consent_given') as bool) as consent_given,
            cast(json_value(payload, '$.returning_caller') as bool) as is_returning_caller,
            cast(json_value(payload, '$.child_age_known') as bool) as is_child_age_known,
            -- Not a boolean: the bot records WHAT KIND of escalation it was,
            -- and the kinds are REFERRAL and HANDOFF. Keeping the type means
            -- the escalation-load metric can separate a referral from a live
            -- transfer, which cost very different things to staff.
            json_value(payload, '$.escalation_flag') as escalation_type,
            json_value(payload, '$.escalation_flag') is not null as was_escalated,
            json_value(payload, '$.menu_choice') as menu_choice,

            -- Populated on a minority of calls by design, not by fault: the bot
            -- can only ask for a rating on calls it finishes itself, and most
            -- callers hang up first. Treat null as "never asked", not "no
            -- opinion", and do not average it without that denominator.
            cast(json_value(payload, '$.csat_rating') as int64) as csat_rating,
            cast(json_value(payload, '$.csat_suspect') as bool) as is_csat_suspect,

            cast(json_value(payload, '$.reprompts') as int64) as reprompt_count,
            cast(json_value(payload, '$.fallbacks') as int64) as fallback_count
        from get_latest_summary_per_call
    )

select *
from select_and_rename_columns
