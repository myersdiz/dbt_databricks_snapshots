{{ config(unique_key="employee_key") }}

select *
  from {{ ref('snp_employee') }}
{{ incremental_where() }}