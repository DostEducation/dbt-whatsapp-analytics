-- Every voice bot analytics event, from both places they live.
--
-- The Cloud Logging sink only collects from the moment it was created, and
-- there is no backfill: everything the bot emitted before 2026-09-19 09:57 UTC
-- would otherwise have aged out of Cloud Logging's 30-day window and been lost.
-- That history was extracted once, under the sink's own filter, and loaded into
-- a partitioned table alongside it.
--
-- Every model downstream reads this, not either table directly, so the join
-- between live and historical data is made once and the seam is invisible.

with
    from_sink as (
        select
            insertId as insert_id,
            timestamp as logged_at,
            to_json_string(jsonPayload) as payload,
            'sink' as source_name
        from {{ source("voicebot", "run_googleapis_com_stdout") }}
    ),

    from_backfill as (
        select
            insertId as insert_id,
            timestamp as logged_at,
            payload_json as payload,
            'backfill' as source_name
        from {{ source("voicebot", "stdout_backfill") }}
    ),

    combined as (
        select * from from_sink
        union all
        select * from from_backfill
    ),

    -- The backfill was cut at the moment the sink began, so the two should not
    -- overlap. Deduplicating on the log entry's own id anyway costs nothing and
    -- means a re-run of the backfill cannot double-count.
    deduplicate_on_log_entry as (
        select *
        from
            (
                select
                    row_number() over (
                        partition by insert_id order by source_name
                    ) as row_number,
                    combined.*
                from combined
            )
        where row_number = 1
    )

select
    insert_id,
    logged_at,
    payload,
    source_name,
    json_value(payload, '$.event') as event_name
from deduplicate_on_log_entry
