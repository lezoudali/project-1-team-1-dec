{% set config = {
    "extract_type": "incremental",
    "incremental_column": "date"
} %}

select
    cast('{{ incremental_value }}' as date) as date,
    sum(cast(HasPrecipitation as int)) as count_precipitations_last_five_days,
    avg('uv_index') as avg_uv_index
from 
    {{ source_table_name }}
{% if is_incremental %}
    where {{ config["incremental_column"] }} >= DATE_SUB('{{ incremental_value }}',INTERVAL 5 DAY)
{% endif %}