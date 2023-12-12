{% snapshot snp_employee %}

    {{
        config(
            unique_key="employee_id",
            surrogate_key="employee_key",
            check_cols=["cdc_hash_key"],
            dbt_current_flag_column="dbt_current_flag",
        )
    }}

    select *
    from {{ ref("stg_employee") }}

{% endsnapshot %}
