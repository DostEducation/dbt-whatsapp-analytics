-- Calls, with the provider's view of the same call joined on and the handful
-- of derivations that more than one metric needs.
--
-- Two sources describe every call and neither is complete alone: the bot knows
-- what happened in the conversation, the provider knows what happened on the
-- line. A full outer join rather than an inner one, because a call that
-- produced only one of the two is still a call that happened -- and dropping
-- it would quietly shrink every denominator built on this model.

with
    calls as (select * from {{ ref("stg_voicebot_calls") }}),

    telephony as (select * from {{ ref("stg_voicebot_telephony") }}),

    combine_bot_and_provider as (
        select
            coalesce(calls.call_sid, telephony.call_sid) as call_sid,

            coalesce(
                calls.call_started_at, telephony.call_started_at
            ) as call_started_at,
            coalesce(
                calls.hashed_phone_prefix, telephony.hashed_phone_prefix
            ) as hashed_phone_prefix,

            -- The provider's duration is the one to trust; the bot's own is the
            -- fallback for calls where no callback arrived.
            coalesce(
                telephony.call_duration_s, calls.call_duration_s
            ) as call_duration_s,

            calls.turn_count,
            calls.final_state,
            calls.phase_at_disconnect,
            calls.consent_given,
            calls.is_returning_caller,
            calls.is_child_age_known,
            calls.menu_choice,
            calls.csat_rating,
            calls.is_csat_suspect,
            calls.reprompt_count,
            calls.fallback_count,
            coalesce(calls.was_escalated, telephony.was_escalated) as was_escalated,
            calls.escalation_type,

            telephony.exotel_number,
            telephony.disconnected_by,
            telephony.recording_url,

            calls.call_sid is not null as has_bot_summary,
            telephony.call_sid is not null as has_provider_callback
        from calls
        full outer join telephony on calls.call_sid = telephony.call_sid
    ),

    add_derivations as (
        select
            *,

            -- Local time is what the M&E team reads. Every date and hour
            -- derived here is IST, so that "calls on the 18th" means the day
            -- the caregiver experienced, not a UTC boundary mid-evening.
            datetime(call_started_at, 'Asia/Kolkata') as call_started_at_ist,
            date(call_started_at, 'Asia/Kolkata') as call_date_ist,
            extract(hour from datetime(call_started_at, 'Asia/Kolkata')) as call_hour_ist,
            date_trunc(date(call_started_at, 'Asia/Kolkata'), week(monday)) as call_week_ist,

            -- A call where no exchange completed. The single largest thing the
            -- pilot could act on, so it gets one definition used everywhere
            -- rather than being re-derived per metric.
            coalesce(turn_count, 0) = 0 as is_zero_turn_call,

            -- Whether the bot ever got the chance to ask for a rating. It can
            -- only ask on calls it ends itself, which is why a plain average of
            -- csat_rating over all calls is meaningless.
            disconnected_by = 'user' as ended_by_caller,

            csat_rating is not null as has_csat_rating
        from combine_bot_and_provider
    )

select *
from add_derivations
