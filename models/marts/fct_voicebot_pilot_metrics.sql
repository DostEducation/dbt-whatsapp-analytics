-- The 23 metrics of the pilot measurement framework's Framework sheet, one
-- table, weekly.
--
-- Grain is (metric_id, period_start, dimension_value). dimension_value is null
-- for a metric that is a single number, and carries the breakdown for one that
-- is a distribution -- a category for 1.1, an hour for 2.4, a phase for 5.3 --
-- so that scalars and distributions live in one table instead of two shapes.
--
-- Every one of the 23 emits a row every week, including the ones that cannot
-- be computed. A metric that silently vanishes reads as zero on a dashboard;
-- a metric that reports itself as needing a human review, or an input nobody
-- has supplied yet, reads as what it is. The status column carries that, and
-- metric_note says what specifically is missing.
--
-- Weekly because that is how M&E slices the pilot. The week starts Monday.

{% set status_computed = "'computed'" %}
{% set status_partial = "'partial'" %}
{% set status_external = "'needs_external_input'" %}
{% set status_review = "'human_review'" %}

with
    calls as (select * from {{ ref("fct_voicebot_calls") }}),
    turns as (select * from {{ ref("fct_voicebot_turns") }}),
    caregivers as (select * from {{ ref("fct_voicebot_caregivers") }}),
    followups as (select * from {{ ref("stg_voicebot_followups") }}),

    -- Every week in which anything happened. Metrics that cannot be computed
    -- still emit against these weeks, so the table has no gaps.
    weeks as (select distinct call_week_ist as period_start from calls),

    -- ---------------------------------------------------------------- track 0
    -- 0.1 Activation rate. The numerator is ours; the denominator is the
    -- onboarded cohort, which exists only in DOST's enrolment records.
    m_0_1 as (
        select
            '0.1' as metric_id,
            'Activation rate' as metric_name,
            '0 Reach' as track,
            weeks.period_start,
            cast(null as string) as dimension_value,
            count(distinct calls.hashed_phone_prefix) as numerator,
            cast(null as float64) as denominator,
            cast(null as float64) as metric_value,
            'share of onboarded families who called' as unit,
            {{ status_external }} as status,
            'Distinct callers is counted here. The denominator is the onboarded cohort, which lives in DOST enrolment records and is not in any app data.' as metric_note
        from weeks
        left join calls on calls.call_week_ist = weeks.period_start
        group by weeks.period_start
    ),

    m_0_2 as (
        select
            '0.2' as metric_id,
            'Onboarding to first call' as metric_name,
            '0 Reach' as track,
            period_start,
            cast(null as string) as dimension_value,
            cast(null as int64) as numerator,
            cast(null as float64) as denominator,
            cast(null as float64) as metric_value,
            'days' as unit,
            {{ status_external }} as status,
            'Needs the date each family was told about the service. Nothing in the app records it.' as metric_note
        from weeks
    ),

    -- ---------------------------------------------------------------- track 1
    m_1_1_agg as (
        select
            call_week_ist as period_start,
            knowledge_module_used as dimension_value,
            count(*) as n
        from turns
        where knowledge_module_used is not null
        group by call_week_ist, knowledge_module_used
    ),

    m_1_1 as (
        select
            '1.1' as metric_id,
            'Category distribution' as metric_name,
            '1 Demand' as track,
            weeks.period_start,
            m_1_1_agg.dimension_value,
            coalesce(m_1_1_agg.n, 0) as numerator,
            cast(
                sum(m_1_1_agg.n) over (partition by weeks.period_start) as float64
            ) as denominator,
            safe_divide(
                m_1_1_agg.n, sum(m_1_1_agg.n) over (partition by weeks.period_start)
            ) as metric_value,
            'share of tagged turns' as unit,
            {{ status_computed }} as status,
            cast(null as string) as metric_note
        -- left join so a week with none of these still emits a row. A metric
        -- that disappears reads as zero on a dashboard; one that reports zero
        -- reads as zero because it was zero.
        from weeks
        left join m_1_1_agg on m_1_1_agg.period_start = weeks.period_start
    ),

    -- 1.2 In-scope rate: turns the bot could route to a knowledge module.
    m_1_2 as (
        select
            '1.2' as metric_id,
            'In-scope rate' as metric_name,
            '1 Demand' as track,
            weeks.period_start,
            cast(null as string) as dimension_value,
            countif(turns.knowledge_module_used is not null) as numerator,
            cast(count(turns.turn_key) as float64) as denominator,
            safe_divide(
                countif(turns.knowledge_module_used is not null), count(turns.turn_key)
            ) as metric_value,
            'share of turns in scope' as unit,
            {{ status_partial }} as status,
            'An untagged turn can mean out of scope, or in scope with no content written yet. The bot does not distinguish the two, so this is an upper bound on what is genuinely out of scope.' as metric_note
        from weeks
        left join turns on turns.call_week_ist = weeks.period_start
        group by weeks.period_start
    ),

    m_1_3 as (
        select
            '1.3' as metric_id,
            'Top recurring questions' as metric_name,
            '1 Demand' as track,
            period_start,
            cast(null as string) as dimension_value,
            cast(null as int64) as numerator,
            cast(null as float64) as denominator,
            cast(null as float64) as metric_value,
            'ranked list' as unit,
            {{ status_external }} as status,
            'Question text is now captured, but ranking questions means grouping them into forms from a controlled vocabulary. That vocabulary is M&E-owned and not yet defined, and the extraction that would apply it is not built.' as metric_note
        from weeks
    ),

    -- ---------------------------------------------------------------- track 2
    -- 2.1 Response latency, as the caregiver experiences it: time until they
    -- hear anything back. Two rows, p50 and p95.
    m_2_1_agg as (
        select
            call_week_ist as period_start,
            approx_quantiles(first_audio_s, 100)[offset(50)] as p50,
            approx_quantiles(first_audio_s, 100)[offset(95)] as p95,
            count(*) as sample_size
        from turns
        where first_audio_s is not null
        group by call_week_ist
    ),

    m_2_1 as (
        select
            '2.1' as metric_id,
            'Response latency' as metric_name,
            '2 Experience' as track,
            m_2_1_agg.period_start,
            percentile.name as dimension_value,
            cast(null as int64) as numerator,
            cast(m_2_1_agg.sample_size as float64) as denominator,
            percentile.value as metric_value,
            'seconds' as unit,
            'partial' as status,
            'Measured from the moment the transcript arrives, not from the end of the caregiver utterance as the framework defines it. The gap between the two is unmeasured, so this understates the true wait.' as metric_note
        from
            m_2_1_agg,
            unnest(
                [
                    struct('p50' as name, m_2_1_agg.p50 as value),
                    struct('p95' as name, m_2_1_agg.p95 as value)
                ]
            ) as percentile
    ),

    -- 2.2 Duration and turns per call. Two rows.
    m_2_2_agg as (
        select
            call_week_ist as period_start,
            approx_quantiles(call_duration_s, 2)[offset(1)] as median_duration_s,
            cast(approx_quantiles(turn_count, 2)[offset(1)] as float64) as median_turns,
            count(*) as sample_size
        from calls
        group by call_week_ist
    ),

    m_2_2 as (
        select
            '2.2' as metric_id,
            'Duration and turns per call' as metric_name,
            '2 Experience' as track,
            m_2_2_agg.period_start,
            measure.name as dimension_value,
            cast(null as int64) as numerator,
            cast(m_2_2_agg.sample_size as float64) as denominator,
            measure.value as metric_value,
            measure.unit,
            'computed' as status,
            cast(null as string) as metric_note
        from
            m_2_2_agg,
            unnest(
                [
                    struct('median_duration' as name, m_2_2_agg.median_duration_s as value, 'seconds' as unit),
                    struct('median_turns' as name, m_2_2_agg.median_turns as value, 'turns' as unit)
                ]
            ) as measure
    ),

    -- 2.3 Cached vs generated. Deliberately reports a count, not a share:
    -- cached_tokens is the cached PROMPT PREFIX while token_count is the
    -- OUTPUT, so dividing one by the other is not a cache-hit rate -- it
    -- produced 138.7 on real data, which is how the mistake surfaced. The
    -- share needs total prompt tokens, which the bot does not log.
    m_2_3 as (
        select
            '2.3' as metric_id,
            'Cached vs generated split' as metric_name,
            '2 Experience' as track,
            weeks.period_start,
            cast(null as string) as dimension_value,
            sum(turns.cached_tokens) as numerator,
            cast(null as float64) as denominator,
            cast(null as float64) as metric_value,
            'cached prompt tokens (count, not a share)' as unit,
            {{ status_partial }} as status,
            'Reports cached prompt tokens only. A cache-hit SHARE needs total prompt tokens as the denominator, and the bot logs the cached prefix and the output count but not the total -- so the share cannot be computed without a new field. The framework also counts the audio cache, and TTS cache hits are not logged at all.' as metric_note
        from weeks
        left join turns on turns.call_week_ist = weeks.period_start
        group by weeks.period_start
    ),

    m_2_4_agg as (
        select
            call_week_ist as period_start,
            lpad(cast(call_hour_ist as string), 2, '0') as dimension_value,
            count(*) as n
        from calls
        where call_hour_ist is not null
        group by call_week_ist, lpad(cast(call_hour_ist as string), 2, '0')
    ),

    m_2_4 as (
        select
            '2.4' as metric_id,
            'Time-of-day distribution' as metric_name,
            '2 Experience' as track,
            weeks.period_start,
            m_2_4_agg.dimension_value,
            coalesce(m_2_4_agg.n, 0) as numerator,
            cast(
                sum(m_2_4_agg.n) over (partition by weeks.period_start) as float64
            ) as denominator,
            safe_divide(
                m_2_4_agg.n, sum(m_2_4_agg.n) over (partition by weeks.period_start)
            ) as metric_value,
            'share of calls in this hour (IST)' as unit,
            {{ status_computed }} as status,
            cast(null as string) as metric_note
        -- left join so a week with none of these still emits a row. A metric
        -- that disappears reads as zero on a dashboard; one that reports zero
        -- reads as zero because it was zero.
        from weeks
        left join m_2_4_agg on m_2_4_agg.period_start = weeks.period_start
    ),

    -- ---------------------------------------------------------------- track 3
    -- 3.1 Repeat callers within seven days. Identified callers only.
    m_3_1 as (
        select
            '3.1' as metric_id,
            '7-day repeat caller rate' as metric_name,
            '3 Retention' as track,
            weeks.period_start,
            cast(null as string) as dimension_value,
            countif(caregivers.is_repeat_caller_within_7d) as numerator,
            cast(count(caregivers.hashed_phone_prefix) as float64) as denominator,
            safe_divide(
                countif(caregivers.is_repeat_caller_within_7d),
                count(caregivers.hashed_phone_prefix)
            ) as metric_value,
            'share of callers who called again within 7 days' as unit,
            {{ status_partial }} as status,
            'Counts only callers the provider gave an ID for. Calls without one cannot be attributed to anyone, so they are outside this denominator -- a property of the telephony, not something the pipeline can fix.' as metric_note
        from weeks
        left join
            caregivers
            on date_trunc(date(caregivers.first_call_at, 'Asia/Kolkata'), week(monday))
            = weeks.period_start
        group by weeks.period_start
    ),

    m_3_2 as (
        select
            '3.2' as metric_id,
            'Calls per family' as metric_name,
            '3 Retention' as track,
            weeks.period_start,
            cast(null as string) as dimension_value,
            count(calls.call_sid) as numerator,
            cast(count(distinct calls.hashed_phone_prefix) as float64) as denominator,
            safe_divide(
                count(calls.call_sid), count(distinct calls.hashed_phone_prefix)
            ) as metric_value,
            'calls per identified caller' as unit,
            {{ status_partial }} as status,
            'Identified callers only, same limit as 3.1.' as metric_note
        from weeks
        left join
            calls
            on calls.call_week_ist = weeks.period_start
            and calls.hashed_phone_prefix is not null
        group by weeks.period_start
    ),

    -- 3.3 Same-day continuation. Unblocked by the follow-up answer event.
    m_3_3 as (
        select
            '3.3' as metric_id,
            'Same-day continuation rate' as metric_name,
            '3 Retention' as track,
            weeks.period_start,
            cast(null as string) as dimension_value,
            countif(followups.did_accept) as numerator,
            cast(
                countif(followups.did_accept or followups.did_decline) as float64
            ) as denominator,
            safe_divide(
                countif(followups.did_accept),
                countif(followups.did_accept or followups.did_decline)
            ) as metric_value,
            'share of read answers that accepted' as unit,
            {{ status_partial }} as status,
            'Denominator is offers whose answer could be read, not all calls: a caller who was never asked did not decline, and an answer the bot could not make out is not a refusal. Offers coming back unclear are excluded from both sides and are worth watching in their own right.' as metric_note
        from weeks
        left join calls on calls.call_week_ist = weeks.period_start
        left join followups on followups.call_sid = calls.call_sid
        group by weeks.period_start
    ),

    -- ---------------------------------------------------------------- track 4
    m_4_1 as (
        select
            '4.1' as metric_id,
            'Word error rate' as metric_name,
            '4 Quality' as track,
            period_start,
            cast(null as string) as dimension_value,
            cast(null as int64) as numerator,
            cast(null as float64) as denominator,
            cast(null as float64) as metric_value,
            'share of words wrong' as unit,
            {{ status_review }} as status,
            'Needs the audio and a human transcription of the same call to compare against. The audio is archived; the transcription is a review task.' as metric_note
        from weeks
    ),

    m_4_2 as (
        select
            '4.2' as metric_id,
            'Entity confirmation rate' as metric_name,
            '4 Quality' as track,
            period_start,
            cast(null as string) as dimension_value,
            cast(null as int64) as numerator,
            cast(null as float64) as denominator,
            cast(null as float64) as metric_value,
            'share confirmed' as unit,
            {{ status_external }} as status,
            'The bot emits no event when it confirms a detail back to the caregiver, so there is nothing to count. Needs a new signal in the app.' as metric_note
        from weeks
    ),

    m_4_3 as (
        select
            '4.3' as metric_id,
            'Task completion rate' as metric_name,
            '4 Quality' as track,
            period_start,
            cast(null as string) as dimension_value,
            cast(null as int64) as numerator,
            cast(null as float64) as denominator,
            cast(null as float64) as metric_value,
            'share of calls completing the task' as unit,
            {{ status_review }} as status,
            'Weekly human review of a sample. The pipeline supplies the sample; the judgement is not automatable.' as metric_note
        from weeks
    ),

    m_4_4 as (
        select
            '4.4' as metric_id,
            'Groundedness' as metric_name,
            '4 Quality' as track,
            period_start,
            cast(null as string) as dimension_value,
            cast(null as int64) as numerator,
            cast(null as float64) as denominator,
            cast(null as float64) as metric_value,
            'share of answers grounded in the knowledge base' as unit,
            {{ status_review }} as status,
            'Same weekly review pass as 4.3.' as metric_note
        from weeks
    ),

    -- 4.5 Perceived usefulness. The capture rate matters more than the score.
    m_4_5_agg as (
        select
            weeks.period_start,
            avg(calls.csat_rating) as mean_rating,
            safe_divide(
                countif(calls.has_csat_rating), count(calls.call_sid)
            ) as capture_rate,
            count(calls.call_sid) as sample_size
        from weeks
        left join calls on calls.call_week_ist = weeks.period_start
        group by weeks.period_start
    ),

    m_4_5 as (
        select
            '4.5' as metric_id,
            'Perceived usefulness' as metric_name,
            '4 Quality' as track,
            m_4_5_agg.period_start,
            measure.name as dimension_value,
            cast(null as int64) as numerator,
            cast(m_4_5_agg.sample_size as float64) as denominator,
            measure.value as metric_value,
            measure.unit,
            'partial' as status,
            'The bot can only ask for a rating on calls it ends itself, and most callers hang up first -- so read capture_rate before mean_rating. A mean over a handful of self-selected ratings is not a satisfaction score.' as metric_note
        from
            m_4_5_agg,
            unnest(
                [
                    struct('mean_rating' as name, m_4_5_agg.mean_rating as value, 'stars' as unit),
                    struct('capture_rate' as name, m_4_5_agg.capture_rate as value, 'share of calls rated' as unit)
                ]
            ) as measure
    ),

    m_4_6_agg as (
        select
            call_week_ist as period_start,
            safety_category as dimension_value,
            count(*) as n
        from turns
        where safety_category is not null
        group by call_week_ist, safety_category
    ),

    m_4_6 as (
        select
            '4.6' as metric_id,
            'Safety triggers and routing' as metric_name,
            '4 Quality' as track,
            weeks.period_start,
            m_4_6_agg.dimension_value,
            coalesce(m_4_6_agg.n, 0) as numerator,
            cast(
                sum(m_4_6_agg.n) over (partition by weeks.period_start) as float64
            ) as denominator,
            safe_divide(
                m_4_6_agg.n, sum(m_4_6_agg.n) over (partition by weeks.period_start)
            ) as metric_value,
            'share of safety turns' as unit,
            {{ status_partial }} as status,
            'Counts what tripped a safety rule. Whether it was then routed correctly is a review judgement, not a field.' as metric_note
        -- left join so a week with none of these still emits a row. A metric
        -- that disappears reads as zero on a dashboard; one that reports zero
        -- reads as zero because it was zero.
        from weeks
        left join m_4_6_agg on m_4_6_agg.period_start = weeks.period_start
    ),

    m_4_7 as (
        select
            '4.7' as metric_id,
            'Fallback trigger rate' as metric_name,
            '4 Quality' as track,
            weeks.period_start,
            cast(null as string) as dimension_value,
            countif(turns.was_fallback_triggered) as numerator,
            cast(count(turns.turn_key) as float64) as denominator,
            safe_divide(
                countif(turns.was_fallback_triggered), count(turns.turn_key)
            ) as metric_value,
            'share of turns falling back' as unit,
            {{ status_computed }} as status,
            cast(null as string) as metric_note
        from weeks
        left join turns on turns.call_week_ist = weeks.period_start
        group by weeks.period_start
    ),

    m_4_8 as (
        select
            '4.8' as metric_id,
            'Out-of-scope refusal quality' as metric_name,
            '4 Quality' as track,
            period_start,
            cast(null as string) as dimension_value,
            cast(null as int64) as numerator,
            cast(null as float64) as denominator,
            cast(null as float64) as metric_value,
            'qualitative' as unit,
            {{ status_review }} as status,
            'Qualitative weekly review. The pipeline supplies the sample.' as metric_note
        from weeks
    ),

    -- ---------------------------------------------------------------- track 5
    m_5_1 as (
        select
            '5.1' as metric_id,
            'Cost per call' as metric_name,
            '5 Cost' as track,
            weeks.period_start,
            cast(null as string) as dimension_value,
            cast(null as int64) as numerator,
            cast(sum(calls.call_duration_s) as float64) as denominator,
            cast(null as float64) as metric_value,
            'currency per call' as unit,
            {{ status_external }} as status,
            'Call seconds are now measured, which was the blocker. Turning them into a cost needs the per-minute telephony, speech and model rates, which are commercial inputs rather than app data.' as metric_note
        from weeks
        left join calls on calls.call_week_ist = weeks.period_start
        group by weeks.period_start
    ),

    m_5_2 as (
        select
            '5.2' as metric_id,
            'Escalation load' as metric_name,
            '5 Cost' as track,
            weeks.period_start,
            cast(null as string) as dimension_value,
            countif(calls.was_escalated) as numerator,
            cast(count(distinct calls.call_date_ist) as float64) as denominator,
            safe_divide(
                countif(calls.was_escalated), count(distinct calls.call_date_ist)
            ) as metric_value,
            'escalations per active day' as unit,
            {{ status_partial }} as status,
            'Counts the load. Whether it fits the staffed capacity needs the roster, which is outside the app.' as metric_note
        from weeks
        left join calls on calls.call_week_ist = weeks.period_start
        group by weeks.period_start
    ),

    m_5_3_agg as (
        select
            call_week_ist as period_start,
            phase_at_disconnect as dimension_value,
            count(*) as n
        from calls
        where phase_at_disconnect is not null
        group by call_week_ist, phase_at_disconnect
    ),

    m_5_3 as (
        select
            '5.3' as metric_id,
            'Drop-off point' as metric_name,
            '5 Cost' as track,
            weeks.period_start,
            m_5_3_agg.dimension_value,
            coalesce(m_5_3_agg.n, 0) as numerator,
            cast(
                sum(m_5_3_agg.n) over (partition by weeks.period_start) as float64
            ) as denominator,
            safe_divide(
                m_5_3_agg.n, sum(m_5_3_agg.n) over (partition by weeks.period_start)
            ) as metric_value,
            'share of calls ending in this phase' as unit,
            {{ status_computed }} as status,
            cast(null as string) as metric_note
        -- left join so a week with none of these still emits a row. A metric
        -- that disappears reads as zero on a dashboard; one that reports zero
        -- reads as zero because it was zero.
        from weeks
        left join m_5_3_agg on m_5_3_agg.period_start = weeks.period_start
    ),

    combine_all_metrics as (
        select * from m_0_1
        union all select * from m_0_2
        union all select * from m_1_1
        union all select * from m_1_2
        union all select * from m_1_3
        union all select * from m_2_1
        union all select * from m_2_2
        union all select * from m_2_3
        union all select * from m_2_4
        union all select * from m_3_1
        union all select * from m_3_2
        union all select * from m_3_3
        union all select * from m_4_1
        union all select * from m_4_2
        union all select * from m_4_3
        union all select * from m_4_4
        union all select * from m_4_5
        union all select * from m_4_6
        union all select * from m_4_7
        union all select * from m_4_8
        union all select * from m_5_1
        union all select * from m_5_2
        union all select * from m_5_3
    )

select
    {{ dbt_utils.generate_surrogate_key([
        "metric_id", "period_start", "coalesce(dimension_value, '~')"
    ]) }} as metric_key,
    *
from combine_all_metrics
