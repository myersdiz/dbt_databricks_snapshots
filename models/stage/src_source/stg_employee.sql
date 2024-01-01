select
    employee_id,
    upper(employee_first_name) as employee_first_name,
    upper(employee_last_name) as employee_last_name,
    hash(employee_id, employee_first_name, employee_last_name) as cdc_hash_key
from {{ source("src_source", "src_employee") }}
