-- One row per INSTRUMENTED conversational turn: what the caregiver said, what
-- the bot decided, what it replied, and how long each stage took.
--
-- Not every turn of a call appears here. turn_index is sparse -- a real call
-- shows 4, 5, 7, 9, 11, 12 -- because only turns that go through the answer
-- path emit these metrics, while consent, menu and other scripted turns do not.
-- So counting rows in this model UNDERCOUNTS the call: use turn_count on
-- stg_voicebot_calls for how long a conversation actually ran, and count rows
-- here only when the question is specifically about answered turns.
--
-- Read as JSON for the same reason as stg_voicebot_calls: the payload schema
-- grows field by field, so a rarely-emitted field is not a column yet.

with
    source as (select * from {{ source("voicebot", "run_googleapis_com_stdout") }}),

    as_json as (
        select
            insertId,
            timestamp as logged_at,
            to_json_string(jsonPayload) as payload
        from source
    ),

    turns as (
        select * from as_json where json_value(payload, '$.event') = 'turn_metrics'
    ),

    -- At-least-once delivery: the same turn can arrive twice. Grain is one row
    -- per turn of a call, so dedupe on that pair rather than on insert_id.
    get_latest_record_per_turn as (
        select *
        from
            (
                select
                    row_number() over (
                        partition by
                            json_value(payload, '$.call_sid'),
                            json_value(payload, '$.turn_index')
                        order by logged_at desc, insertId desc
                    ) as row_number,
                    turns.*
                from turns
            )
        where row_number = 1
    ),

    select_and_rename_columns as (
        select
            {{ dbt_utils.generate_surrogate_key([
                "json_value(payload, '$.call_sid')",
                "json_value(payload, '$.turn_index')"
            ]) }} as turn_key,
            json_value(payload, '$.call_sid') as call_sid,
            cast(json_value(payload, '$.turn_index') as int64) as turn_index,
            logged_at,

            json_value(payload, '$.hashed_phone_prefix') as hashed_phone_prefix,

            -- What was actually said. caregiver_said is verbatim speech from a
            -- real caller and may contain anything they chose to say; it is the
            -- most sensitive column in this project. bot_said is only populated
            -- from 16 Sep 2026, when recording the bot's own words was switched
            -- on -- earlier turns cannot be backfilled.
            json_value(payload, '$.transcript') as caregiver_said,
            json_value(payload, '$.response') as bot_said,

            -- What kind of turn this was: a generated answer, a scripted
            -- template, a safety response, consent, or end-of-call.
            json_value(payload, '$.decision') as turn_decision,
            cast(json_value(payload, '$.confidence') as float64) as stt_confidence,

            json_value(payload, '$.knowledge_module_used') as knowledge_module_used,
            json_value(payload, '$.knowledge_router_turn_tag') as knowledge_router_tag,
            json_value(payload, '$.safety_category') as safety_category,

            -- Latency, split by stage. latency_s is the whole turn;
            -- first_audio_s is what the caregiver actually experiences as the
            -- wait before hearing anything back.
            cast(json_value(payload, '$.latency_s') as float64) as turn_latency_s,
            cast(json_value(payload, '$.first_audio_s') as float64) as first_audio_s,
            cast(
                json_value(payload, '$.stt_speech_final_s') as float64
            ) as stt_speech_final_s,
            cast(
                json_value(payload, '$.llm_first_token_s') as float64
            ) as llm_first_token_s,
            cast(
                json_value(payload, '$.first_tts_chunk_s') as float64
            ) as first_tts_chunk_s,
            cast(
                json_value(payload, '$.playback_duration_s') as float64
            ) as playback_duration_s,

            cast(json_value(payload, '$.token_count') as int64) as token_count,
            cast(json_value(payload, '$.cached_tokens') as int64) as cached_tokens,

            cast(
                json_value(payload, '$.fallback_triggered') as bool
            ) as was_fallback_triggered,
            json_value(payload, '$.fallback_reason') as fallback_reason,
            cast(json_value(payload, '$.escalation') as bool) as was_escalated,

            json_value(payload, '$.tts_speaker') as tts_speaker,
            cast(
                json_value(payload, '$.tts_fallback_count') as int64
            ) as tts_fallback_count
        from get_latest_record_per_turn
    )

select *
from select_and_rename_columns
