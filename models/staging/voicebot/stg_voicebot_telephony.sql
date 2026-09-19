-- One row per call, from what the telephony provider reports after it ends.
--
-- Exotel sends its callbacks to two different endpoints with two different
-- parameter shapes, and neither alone is complete: the status callback carries
-- the billed duration and when the recording will be ready, while the stream
-- callback carries who hung up and the recording URL that actually resolves.
-- Reading only one of them is what made call duration look absent for weeks.
-- This model puts both back together, one row per call.

with
    source as (select * from {{ source("voicebot", "run_googleapis_com_stdout") }}),

    as_json as (
        select
            insertId,
            timestamp as logged_at,
            to_json_string(jsonPayload) as payload
        from source
    ),

    latest_per_call_and_event as (
        select *
        from
            (
                select
                    row_number() over (
                        partition by
                            json_value(payload, '$.call_sid'),
                            json_value(payload, '$.event')
                        order by logged_at desc, insertId desc
                    ) as row_number,
                    as_json.*
                from as_json
                where
                    json_value(payload, '$.event')
                    in ('exotel_status', 'exotel_escalation')
            )
        where row_number = 1
    ),

    status_callback as (
        select
            json_value(payload, '$.call_sid') as call_sid,
            logged_at as status_logged_at,

            -- exotel_number is the DOST line the caregiver dialled, not the
            -- caregiver's own number. Two are in use across the pilot and no
            -- call mixes them, so it cleanly separates the entry points.
            json_value(payload, '$.exotel_number') as exotel_number,
            json_value(payload, '$.hashed_phone_prefix') as hashed_phone_prefix,
            json_value(payload, '$.direction') as call_direction,
            json_value(payload, '$.call_type') as call_type,
            cast(json_value(payload, '$.start_time') as timestamp) as call_started_at,

            -- Measured empirically as 0 on every status callback seen so far
            -- (47 of 47 over three days). Kept only so that a future change at
            -- the provider's end shows up rather than being silently dropped;
            -- the duration to actually use is stream_duration_s below.
            cast(
                json_value(payload, '$.dial_call_duration') as float64
            ) as dial_call_duration_s,

            json_value(payload, '$.recording_url') as status_recording_url,
            cast(
                json_value(payload, '$.recording_available_by') as float64
            ) as recording_available_by_s
        from latest_per_call_and_event
        where json_value(payload, '$.event') = 'exotel_status'
    ),

    stream_callback as (
        select
            json_value(payload, '$.call_sid') as call_sid,
            logged_at as stream_logged_at,
            json_value(payload, '$.stream_sid') as stream_sid,
            json_value(payload, '$.stream_status') as stream_status,
            cast(json_value(payload, '$.stream_duration') as float64) as stream_duration_s,

            -- Who ended the call. The clearest signal of an abandoned call
            -- that exists anywhere in this pipeline.
            json_value(payload, '$.stream_disconnected_by') as disconnected_by,
            -- This, not dial_call_duration, is where the provider reports how
            -- long the call ran: populated on 47 of 47 callbacks, and matching
            -- the bot's own measurement to within a second.
            json_value(payload, '$.stream_recording_url') as stream_recording_url,
            cast(json_value(payload, '$.escalate') as bool) as was_escalated
        from latest_per_call_and_event
        where json_value(payload, '$.event') = 'exotel_escalation'
    ),

    -- Full outer join: a call can produce either callback without the other,
    -- and dropping the half that arrived alone would silently lose the call.
    combine_both_callbacks as (
        select
            coalesce(status_callback.call_sid, stream_callback.call_sid) as call_sid,
            status_callback.exotel_number,
            status_callback.hashed_phone_prefix,
            status_callback.call_direction,
            status_callback.call_type,
            status_callback.call_started_at,
            status_callback.dial_call_duration_s,
            status_callback.recording_available_by_s,
            stream_callback.stream_sid,
            stream_callback.stream_status,
            stream_callback.stream_duration_s,
            stream_callback.disconnected_by,

            -- One duration for downstream models to use, so no consumer has to
            -- know which callback carries what. Prefers the stream callback
            -- because that is the one the provider actually populates.
            coalesce(
                nullif(stream_callback.stream_duration_s, 0),
                nullif(status_callback.dial_call_duration_s, 0)
            ) as call_duration_s,
            stream_callback.was_escalated,

            -- The stream URL is the one that resolves; the status URL is kept
            -- as a fallback for calls where only that callback arrived.
            coalesce(
                stream_callback.stream_recording_url, status_callback.status_recording_url
            ) as recording_url,

            status_callback.status_logged_at is not null as has_status_callback,
            stream_callback.stream_logged_at is not null as has_stream_callback
        from status_callback
        full outer join stream_callback on status_callback.call_sid = stream_callback.call_sid
    )

select *
from combine_both_callbacks
