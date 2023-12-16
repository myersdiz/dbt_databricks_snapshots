select
    supplier_id,
    upper(supplier_name) as supplier_name,
    hash(supplier_id, supplier_name) as cdc_hash_key
from {{ source("src_hive_metastore_default", "supplier") }}
