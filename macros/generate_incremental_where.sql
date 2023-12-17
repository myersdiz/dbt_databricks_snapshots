{%- macro generate_incremental_where() -%}
  {%- if is_incremental() -%}
 WHERE COALESCE(dbt_valid_to, dbt_valid_from) > (SELECT MAX(COALESCE(NULLIF(dbt_valid_to,'9999-12-31 23:59:59.999'::TIMESTAMP), dbt_valid_from)) FROM {{ this }})
  {%- endif -%}
{%- endmacro -%}