-- One row per voice bot call. The table to start from for anything call-shaped:
-- reach, drop-off, duration, satisfaction, escalation.

with
    call_facts as (select * from {{ ref("int_voicebot_call_facts") }}),

    turns as (select * from {{ ref("stg_voicebot_turns") }}),

    -- Per-turn detail rolled up to the call. Counted from instrumented turns
    -- only, which is why these are named *_instrumented -- turn_count from the
    -- summary remains the authoritative length of the conversation.
    turn_rollup as (
        select
            call_sid,
            count(*) as instrumented_turn_count,
            countif(caregiver_said is not null) as turns_with_caregiver_text,
            countif(bot_said is not null) as turns_with_bot_text,
            countif(safety_category is not null) as safety_turn_count,
            countif(was_fallback_triggered) as fallback_turn_count,
            count(distinct knowledge_module_used) as distinct_modules_used,

            -- What the caller experiences as waiting. Median over the call,
            -- not the mean, because one slow turn should not redefine the call.
            approx_quantiles(first_audio_s, 2)[offset(1)] as median_first_audio_s,
            approx_quantiles(turn_latency_s, 2)[offset(1)] as median_turn_latency_s,
            max(turn_latency_s) as max_turn_latency_s,

            sum(token_count) as total_tokens,
            sum(cached_tokens) as total_cached_tokens
        from turns
        group by call_sid
    ),

    combine as (
        select
            call_facts.*,
            coalesce(turn_rollup.instrumented_turn_count, 0) as instrumented_turn_count,
            coalesce(turn_rollup.turns_with_caregiver_text, 0) as turns_with_caregiver_text,
            coalesce(turn_rollup.turns_with_bot_text, 0) as turns_with_bot_text,
            coalesce(turn_rollup.safety_turn_count, 0) as safety_turn_count,
            coalesce(turn_rollup.fallback_turn_count, 0) as fallback_turn_count,
            turn_rollup.distinct_modules_used,
            turn_rollup.median_first_audio_s,
            turn_rollup.median_turn_latency_s,
            turn_rollup.max_turn_latency_s,
            turn_rollup.total_tokens,
            turn_rollup.total_cached_tokens
        from call_facts
        left join turn_rollup on call_facts.call_sid = turn_rollup.call_sid
    )

select *
from combine
