{% snapshot snp_supplier %}

    {{
        config(
            unique_key="supplier_id",
            surrogate_key="supplier_key",
            check_cols=['cdc_hash_key'],
            dbt_current_flag_column="dbt_current_flag"
        )
    }}

    select *
    from {{ ref("stg_supplier") }}

{% endsnapshot %}
