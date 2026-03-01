{{ config(
    materialized='incremental',
    unique_key=['spid', 'category_id'],
    on_schema_change='sync_all_columns',
    pre_hook=[
        "ALTER TABLE IF EXISTS {{ this }} DROP CONSTRAINT IF EXISTS fk_pc_product",
        "ALTER TABLE IF EXISTS {{ this }} DROP CONSTRAINT IF EXISTS fk_pc_category"
    ],
    post_hook=[
        """
        DO $$ BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint c
                JOIN pg_class t     ON t.oid = c.conrelid
                JOIN pg_namespace n ON n.oid = t.relnamespace
                WHERE c.contype = 'p'
                  AND t.relname  = 'product_categories'
                  AND n.nspname  = 'cleaned'
            ) THEN
                ALTER TABLE {{ this }} ADD PRIMARY KEY (spid, category_id);
            END IF;
        END $$;
        """,
        """
        DO $$ BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint c
                JOIN pg_class t     ON t.oid = c.conrelid
                JOIN pg_namespace n ON n.oid = t.relnamespace
                WHERE c.conname  = 'fk_pc_product'
                  AND t.relname  = 'product_categories'
                  AND n.nspname  = 'cleaned'
            ) THEN
                ALTER TABLE {{ this }}
                ADD CONSTRAINT fk_pc_product
                FOREIGN KEY (spid) REFERENCES {{ ref('products') }} (spid)
                ON DELETE CASCADE;
            END IF;
        END $$;
        """,
        """
        DO $$ BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint c
                JOIN pg_class t     ON t.oid = c.conrelid
                JOIN pg_namespace n ON n.oid = t.relnamespace
                WHERE c.conname  = 'fk_pc_category'
                  AND t.relname  = 'product_categories'
                  AND n.nspname  = 'cleaned'
            ) THEN
                ALTER TABLE {{ this }}
                ADD CONSTRAINT fk_pc_category
                FOREIGN KEY (category_id) REFERENCES {{ ref('categories') }} (category_id)
                ON DELETE CASCADE;
            END IF;
        END $$;
        """
    ]
) }}


WITH raw_items AS (
    SELECT
        (item->>'seller_product_id')::BIGINT    AS spid,
        item->>'primary_category_path'          AS category_path,
        extract_time
    FROM {{ source('raw', 'raw_product_listings') }},
    LATERAL jsonb_array_elements(raw_response->'data') AS item
    WHERE item->>'primary_category_path' IS NOT NULL
),

exploded AS (
    SELECT DISTINCT
        spid,
        segment::INT AS category_id
    FROM raw_items,
    LATERAL unnest(string_to_array(category_path, '/')) AS segment
    WHERE segment ~ '^\d+$'
      AND segment::INT != 0
),

validated AS (
    SELECT e.spid, e.category_id
    FROM exploded e
    INNER JOIN {{ ref('categories') }} c ON c.category_id = e.category_id
)

SELECT
    v.spid,
    v.category_id
FROM validated v
INNER JOIN {{ ref('products') }} p ON p.spid = v.spid

{% if is_incremental() %}
    WHERE (v.spid, v.category_id) NOT IN (
        SELECT spid, category_id FROM {{ this }}
    )
{% endif %}