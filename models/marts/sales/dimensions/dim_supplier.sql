{{ config(unique_key="supplier_key") }}

select *
  from {{ ref('snp_supplier') }}
{{ incremental_where() }}