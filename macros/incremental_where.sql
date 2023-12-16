{%- macro incremental_where() -%}
  {%- if is_incremental() -%}
 WHERE COALESCE(dbt_valid_to, dbt_valid_from) > (SELECT MAX(COALESCE(dbt_valid_to, dbt_valid_from)) FROM {{ this }})
  {%- endif -%}
{%- endmacro -%}