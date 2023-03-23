with
    flows_on_glific as (select * from {{ ref('stg_flows') }}),

    flows_on_google_sheet as (select * from {{ ref('stg_flow_configs') }}),

    join_tables as (
        select
            flows_on_glific.*,
            flows_on_google_sheet.* except (flow_uuid, flow_name)
        from
            flows_on_glific
            full outer join flows_on_google_sheet using (flow_uuid)
    ),

    add_flow_status as (
        select
            *,
            if (flow_sr_no is null, false, true) as configured_on_google_sheet
        from join_tables
    )

select * from add_flow_status