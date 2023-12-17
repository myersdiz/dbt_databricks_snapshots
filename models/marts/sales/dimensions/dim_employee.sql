{{ config(unique_key="employee_key") }}

select {{ dbt_utils.star(from=ref('snp_employee'), except=["cdc_hash_key","dbt_scd_id","dbt_updated_at","dbt_valid_from","dbt_valid_to","dbt_current_flag"]) }}
       {{ generate_scd_columns() }}
  from {{ ref('snp_employee') }}
{{ generate_incremental_where() }}

{{ generate_n_a_row(model_name='snp_employee', exclude_col_list=["cdc_hash_key","dbt_scd_id","dbt_updated_at","dbt_valid_from","dbt_valid_to","dbt_current_flag"]) }}