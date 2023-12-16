{%- macro generate_scd_columns() -%}
,dbt_updated_at
,dbt_valid_from
,COALESCE(timestampadd(MILLISECOND,-1,dbt_valid_to),'9999-12-31 23:59:59.999'::TIMESTAMP) AS dbt_valid_to
,dbt_current_flag
{%- endmacro -%}