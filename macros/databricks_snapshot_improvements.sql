{% macro get_snapshot_config() %}
    {{ return(adapter.dispatch('get_snapshot_config')()) }}
{% endmacro %}

{% macro default__get_snapshot_config() -%}
    {%- set config = model['config'] -%}
    {%- set surrogate_key = config.get("surrogate_key") -%}
    {% do return({
            "unique_key": config.get("unique_key"),
            "surrogate_key": surrogate_key,
            "dbt_current_flag_column": config.get("dbt_current_flag_column")
        }) %}
{% endmacro %}

{% macro databricks__snapshot_staging_table(strategy, source_sql, target_relation) -%}
    {%- set config = get_snapshot_config() -%}

    {% if execute and config.surrogate_key %}
        {%- set surrogate_key_max_value_statement -%}
        select max({{ config.surrogate_key }}) from {{ target_relation }}
        {%- endset -%}
        {%- set results = run_query(surrogate_key_max_value_statement) -%}
        {%- set surrogate_key_max_value = results.columns[0].values()[0] -%}
    {%- endif %}

    with snapshot_query as (

        {{ source_sql }}

    ),

    snapshotted_data as (

        select
            *,
            {{ strategy.unique_key }} as dbt_unique_key
        from {{ target_relation }}
        where dbt_valid_to is null

    ),

    insertions_source_data as (

        select
            *,
            {{ strategy.unique_key }} as dbt_unique_key,
            {{ strategy.updated_at }} as dbt_updated_at,
            {{ strategy.updated_at }} as dbt_valid_from,
            nullif({{ strategy.updated_at }}, {{ strategy.updated_at }}) as dbt_valid_to,

            {% if config.dbt_current_flag_column -%}
                'Y' as {{config.dbt_current_flag_column}},
            {%- endif %}

            {{ strategy.scd_id }} as dbt_scd_id

        from snapshot_query

    ),

    updates_source_data as (

        select
            *,
            {{ strategy.unique_key }} as dbt_unique_key,
            {{ strategy.updated_at }} as dbt_updated_at
        from snapshot_query
    ),


    insertions as (

        select
            'insert' as dbt_change_type,

            {% if config.surrogate_key -%}
                -- Surrogate key from sequence
                row_number() over (partition by null order by monotonically_increasing_id()) + {{ surrogate_key_max_value }} as {{config.surrogate_key}},
            {%- endif %}

            source_data.*

        from insertions_source_data as source_data
        left outer join snapshotted_data on snapshotted_data.dbt_unique_key = source_data.dbt_unique_key
        where snapshotted_data.dbt_unique_key is null
           or (
                snapshotted_data.dbt_unique_key is not null
            and (
                {{ strategy.row_changed }}
            )
        )

    ),

    updates as (

        select
            'update' as dbt_change_type,

            {% if config.surrogate_key -%}
                -- Surrogate key from target
                snapshotted_data.{{config.surrogate_key}},
            {% endif %}

            source_data.*,
            snapshotted_data.dbt_valid_from,
            source_data.dbt_updated_at as dbt_valid_to,

            {% if config.dbt_current_flag_column -%}
                'N' as {{config.dbt_current_flag_column}},
            {%- endif %}

            snapshotted_data.dbt_scd_id

        from updates_source_data as source_data
        inner join snapshotted_data on snapshotted_data.dbt_unique_key = source_data.dbt_unique_key
        where (
            {{ strategy.row_changed }}
        )
    )

    {%- if strategy.invalidate_hard_deletes -%}
    ,

    deletes as (

        select
            'delete' as dbt_change_type,

            {% if config.surrogate_key -%}
                -- Surrogate key from target
                snapshotted_data.{{config.surrogate_key}},
            {%- endif %}

            source_data.*,
            snapshotted_data.dbt_unique_key,
            {{ snapshot_get_time() }} as dbt_updated_at,
            snapshotted_data.dbt_valid_from,
            {{ snapshot_get_time() }} as dbt_valid_to,

            {% if config.dbt_current_flag_column -%}
                'N' as {{config.dbt_current_flag_column}},
            {%- endif %}

            snapshotted_data.dbt_scd_id

        from snapshotted_data
        left outer join snapshot_query as source_data on snapshotted_data.dbt_unique_key = source_data.{{ strategy.unique_key }}
        where source_data.{{ strategy.unique_key }} is null
    )
    {%- endif %}

    select * from insertions
    union all
    select * from updates
    {%- if strategy.invalidate_hard_deletes %}
    union all
    select * from deletes
    {%- endif %}

{%- endmacro %}

{% macro databricks__build_snapshot_table(strategy, sql) %}
    {%- set config = get_snapshot_config() -%}

    select
        {% if config.surrogate_key -%}
            row_number() over (partition by null order by monotonically_increasing_id()) as {{config.surrogate_key}},
        {%- endif %}
        *,
        {{ strategy.scd_id }} as dbt_scd_id,
        {{ strategy.updated_at }} as dbt_updated_at,
        {{ strategy.updated_at }} as dbt_valid_from,
        nullif({{ strategy.updated_at }}, {{ strategy.updated_at }}) as dbt_valid_to
        {%- if config.dbt_current_flag_column -%},
        'Y' AS {{ config.dbt_current_flag_column }}
        {%- endif %}
    from (
        {{ sql }}
    ) sbq

{% endmacro %}

{% macro databricks__snapshot_merge_sql(target, source, insert_cols) -%}
    {%- set config = get_snapshot_config() -%}
    {%- set insert_cols_csv = insert_cols | join(', ') -%}

    merge into {{ target }} as DBT_INTERNAL_DEST
    using {{ source }} as DBT_INTERNAL_SOURCE
        on DBT_INTERNAL_SOURCE.dbt_scd_id = DBT_INTERNAL_DEST.dbt_scd_id

        -- Databricks is likely to prune better on the dbt_unique_key or dbt_valid_from
        and DBT_INTERNAL_SOURCE.dbt_unique_key = DBT_INTERNAL_DEST.{{config.unique_key}}
        and DBT_INTERNAL_SOURCE.dbt_valid_from = DBT_INTERNAL_DEST.dbt_valid_from

        {% if config.surrogate_key -%}
        -- Databricks is also likely to prune well on a sequence-based surrogate key
        and DBT_INTERNAL_SOURCE.{{config.surrogate_key}} = DBT_INTERNAL_DEST.{{config.surrogate_key}}
        {%- endif %}

    when matched
     and DBT_INTERNAL_DEST.dbt_valid_to is null
     and DBT_INTERNAL_SOURCE.dbt_change_type in ('update', 'delete')
        then update
        set dbt_valid_to = DBT_INTERNAL_SOURCE.dbt_valid_to

        {%- if config.dbt_current_flag_column -%}
           ,{{ config.dbt_current_flag_column }} = DBT_INTERNAL_SOURCE.{{config.dbt_current_flag_column}}
        {%- endif %}

    when not matched
     and DBT_INTERNAL_SOURCE.dbt_change_type = 'insert'
        then insert ({{ insert_cols_csv }})
        values ({{ insert_cols_csv }})

{% endmacro %}