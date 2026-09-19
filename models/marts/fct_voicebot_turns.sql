-- One row per instrumented turn, with enough of its call's context attached
-- that turn-level questions can be answered without a join.

with
    turns as (select * from {{ ref("stg_voicebot_turns") }}),

    call_facts as (select * from {{ ref("int_voicebot_call_facts") }}),

    combine as (
        select
            turns.*,

            call_facts.call_started_at,
            call_facts.call_date_ist,
            call_facts.call_week_ist,
            call_facts.call_hour_ist,
            call_facts.call_duration_s,
            call_facts.turn_count as call_turn_count,
            call_facts.consent_given,
            call_facts.phase_at_disconnect,
            call_facts.exotel_number,
            call_facts.disconnected_by,

            -- Whether this was the last instrumented turn of its call. The
            -- drop-off question is asked of this column, not of a max() at
            -- read time.
            turns.turn_index = max(turns.turn_index) over (
                partition by turns.call_sid
            ) as is_last_instrumented_turn
        from turns
        left join call_facts on turns.call_sid = call_facts.call_sid
    )

select *
from combine
