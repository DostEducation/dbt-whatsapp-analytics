{% set field_names_query %}
    SELECT DISTINCT field_name FROM {{ ref('stg_user_attributes') }}
{% endset %}

with
    user_attributes_cte as (
        select
            user_id,
            {% set field_names = run_query(field_names_query) %}
            {% for field_name in field_names %}
                max(
                    case
                        when field_name = '{{ field_name.field_name }}'
                        then field_value
                        else null
                    end
                ) as {{ field_name.field_name }}
                {% if not loop.last %},{% endif %}
            {% endfor %}
        from {{ ref("stg_user_attributes") }}
        group by user_id
    )

select gu.*, ua.* except(user_id)
from {{ ref("stg_users") }} gu
left join user_attributes_cte ua on gu.user_id = ua.user_id
where gu.user_id is not null
