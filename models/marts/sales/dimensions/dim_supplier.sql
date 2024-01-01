{{ config(unique_key="supplier_key") }}

select
    {{
        dbt_utils.star(
            from=ref("snp_supplier"),
            except=[
                "cdc_hash_key",
                "dbt_scd_id",
                "dbt_updated_at",
                "dbt_valid_from",
                "dbt_valid_to",
                "dbt_current_flag",
            ],
        )
    }}
    {{ generate_scd_columns() }}
from
    {{ ref("snp_supplier") }}
    {{ generate_incremental_where() }}

    {{
        generate_n_a_row(
            model_name="snp_supplier",
            exclude_col_list=[
                "cdc_hash_key",
                "dbt_scd_id",
                "dbt_updated_at",
                "dbt_valid_from",
                "dbt_valid_to",
                "dbt_current_flag",
            ],
        )
    }}
