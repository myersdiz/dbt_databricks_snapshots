{%- macro generate_n_a_row(model_name, column_name, exclude_col_list=None, key_value_dict=None) -%}
  {# Initialize the lists #}
  {%- set exclude_cols = exclude_col_list if exclude_col_list is not none else [] -%}
  {%- set key_value_dict = key_value_dict if key_value_dict is not none else {} -%}

  {# Convert all column names in the exclude_cols list to lowercase #}
  {%- set exclude_cols = exclude_cols | map('lower') | list -%}

  {# Retrieve the list of columns from the model #}
  {%- set model_cols = adapter.get_columns_in_relation(ref(model_name)) -%}

  {%- if not is_incremental() -%}
union all

select 
    {% for col in model_cols %}
        {%- set col_name = col.column.lower() -%}
        {%- if col_name not in exclude_cols %}
            {%- set default_value = "'N/A'" if col.data_type == 'string'
                               else "0::int" if col.data_type == 'int'
                               else "0::bigint" if col.data_type == 'bigint'
                               else "NULL" %}
            {%- set new_value = key_value_dict[col_name.lower()] if col_name.lower() in key_value_dict else default_value -%}
    {{ new_value }} as {{ col_name }},
        {% endif -%}
    {% endfor %}
    current_timestamp() as dbt_updated_at,
    current_timestamp() as dbt_valid_from,
    '9999-12-31 23:59:59.999'::timestamp as dbt_valid_to,
    'Y' as dbt_current_flag
  {%- endif -%}

{% endmacro %}