{% set config = {
    "extract_type": "incremental",
    "incremental_column": "date"
} %}

select
    cast('{{ incremental_value }}' as date) as date,
    sum(cast(has_precipitation as int)) as count_precipitations_last_five_days
from 
    {{ source_table_name }}
{% if is_incremental %}
    where {{ config["incremental_column"] }} >= current_date - INTERVAL '5 DAYS'
{% endif %}