with
    flow_results as (select * from {{ ref('stg_flow_results') }}),

    nodes as (select * from {{ ref('int_nodes') }}),

    configured_responses as (select * from {{ ref('stg_configured_responses') }}),

    outputs as (select * from {{ ref('stg_outputs') }}),

    contacts as (select * from {{ ref('fct_contacts') }}),

    add_node_details as (
        select
            flow_results.*,
            nodes.* except (flow_uuid)
        from
            flow_results
            left join nodes on flow_results.result_key =  nodes.node_label
    ),

    add_desired_response as (
        select
            add_node_details.*,
            configured_responses.configured_response_id,
            configured_responses.desired_response,
        from
            add_node_details
            left join configured_responses using (node_label, result_category)
    ),

    add_output_label as (
        select
            add_desired_response.*,
            outputs.output_name
        from
            add_desired_response
            left join outputs using (output_id)
    ),

    add_user_type as (
        select
            add_output_label.*,
            contacts.user_type
        from
            add_output_label
            left join contacts using (contact_phone)
    )

select * from add_user_type




