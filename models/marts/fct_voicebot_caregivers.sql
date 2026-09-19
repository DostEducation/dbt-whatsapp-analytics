-- One row per identified caregiver, keyed by the salted hash prefix.
--
-- Calls where the provider sent no caller ID have no key and are excluded
-- here -- they are real calls and stay in fct_voicebot_calls, but they cannot
-- be attributed to anyone. That exclusion is the reason the repeat-caller
-- metrics carry a smaller denominator than the call count, and it is a
-- property of the telephony, not something the pipeline can improve.

with
    calls as (
        select *
        from {{ ref("int_voicebot_call_facts") }}
        where hashed_phone_prefix is not null
    ),

    aggregate_by_caregiver as (
        select
            hashed_phone_prefix,

            count(*) as total_calls,
            min(call_started_at) as first_call_at,
            max(call_started_at) as last_call_at,
            count(distinct call_date_ist) as distinct_days_called,

            sum(turn_count) as total_turns,
            avg(turn_count) as mean_turns_per_call,
            approx_quantiles(call_duration_s, 2)[offset(1)] as median_call_duration_s,
            sum(call_duration_s) as total_duration_s,

            countif(is_zero_turn_call) as zero_turn_calls,
            countif(consent_given) as calls_with_consent,
            countif(was_escalated) as escalated_calls,
            countif(has_csat_rating) as rated_calls,
            avg(csat_rating) as mean_csat_rating,

            -- Days between first and last call. Zero for a caller who has only
            -- ever called once, which is why repeat rate is counted from
            -- total_calls rather than from this.
            date_diff(
                max(call_date_ist), min(call_date_ist), day
            ) as days_between_first_and_last_call
        from calls
        group by hashed_phone_prefix
    ),

    add_flags as (
        select
            *,
            total_calls > 1 as is_repeat_caller,

            -- Repeat within a week of first calling: the framework's 3.1
            -- definition, evaluated per caregiver so the metric model can
            -- simply count them.
            days_between_first_and_last_call
            between 1 and 7 as is_repeat_caller_within_7d
        from aggregate_by_caregiver
    )

select *
from add_flags
