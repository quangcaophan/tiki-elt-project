{{ config(
    materialized='incremental',
    unique_key='category_id',
    on_schema_change='sync_all_columns',
    pre_hook="ALTER TABLE IF EXISTS {{ this }} DROP CONSTRAINT IF EXISTS fk_category_parent",
    post_hook=[
        """
        DO $$ BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint c
                JOIN pg_class t     ON t.oid = c.conrelid
                JOIN pg_namespace n ON n.oid = t.relnamespace
                WHERE c.contype = 'p'
                  AND t.relname  = 'categories'
                  AND n.nspname  = 'cleaned'
            ) THEN
                ALTER TABLE {{ this }} ADD PRIMARY KEY (category_id);
            END IF;
        END $$;
        """
    ]
) }}

SELECT
    (value->>'id')::INT                 AS category_id,
    (value->'parent_id')::INT           AS parent_id,
    value->>'name'                      AS name,
    value->>'url_key'                   AS url_key,
    value->>'url_path'                  AS url_path,
    (value->>'level')::INT              AS level,
    value->>'status'                    AS status,
    (value->>'is_leaf')::BOOLEAN        AS is_leaf,
    (value->>'product_count')::INT      AS product_count,
    value->>'thumbnail_url'             AS thumbnail_url,
    value->>'meta_title'                AS meta_title,
    value->>'meta_description'          AS meta_description
FROM {{ source('raw', 'raw_categories') }},
LATERAL jsonb_array_elements(raw_response) AS t
WHERE raw_response::TEXT != '[]'

{% if is_incremental() %}
    AND (value->>'id')::INT NOT IN (SELECT category_id FROM {{ this }})
{% endif %}

ORDER BY (value->>'level')::INT ASC