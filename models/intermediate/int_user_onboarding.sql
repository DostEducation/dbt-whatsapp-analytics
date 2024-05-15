{% set field_names_query %}
    SELECT DISTINCT field_name FROM {{ ref('stg_user_attributes') }}
{% endset %}

with
    user_attributes as (
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

select users.*, user_attributes.* except(user_id)
from {{ ref("stg_users") }} users
left join user_attributes on users.id = user_attributes.user_id
where users.id is not null
