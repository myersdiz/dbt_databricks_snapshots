{{ config(unique_key="employee_key") }}

select {{ dbt_utils.star(from=ref('snp_employee'), except=["cdc_hash_key","dbt_scd_id","dbt_updated_at","dbt_valid_from","dbt_valid_to","dbt_current_flag"]) }}
       {{ generate_scd_columns() }}
  from {{ ref('snp_employee') }}
{{ incremental_where() }}