{{ config(
    materialized='incremental',
    unique_key='review_id',
    on_schema_change='sync_all_columns',
    pre_hook=[
        "ALTER TABLE IF EXISTS {{ this }} DROP CONSTRAINT IF EXISTS fk_review_product",
        "ALTER TABLE IF EXISTS {{ this }} DROP CONSTRAINT IF EXISTS fk_review_seller"
    ],
    post_hook=[
        """
        DO $$ BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint c
                JOIN pg_class t     ON t.oid = c.conrelid
                JOIN pg_namespace n ON n.oid = t.relnamespace
                WHERE c.contype = 'p'
                  AND t.relname  = 'reviews'
                  AND n.nspname  = 'cleaned'
            ) THEN
                ALTER TABLE {{ this }} ADD PRIMARY KEY (review_id);
            END IF;
        END $$;
        """
    ]
) }}

SELECT
    (r->>'id')::BIGINT                                          AS review_id,
    (r->>'product_id')::BIGINT                                  AS product_id,
    (r->>'spid')::BIGINT                                        AS spid,
    (r->'seller'->>'id')::INT                                   AS seller_id,
    (r->'created_by'->>'id')::BIGINT                            AS user_id,
    (r->'created_by'->>'name')                                  AS user_name,
    (r->>'rating')::INT                                         AS rating,
    (r->>'title')                                               AS title,
    (r->>'content')                                             AS content,
    to_timestamp((r->'created_by'->>'purchased_at')::BIGINT)    AS purchased_at,
    (r->'product_attributes'->>0)                               AS variant,
    (r->>'thank_count')::INT                                    AS thank_count
FROM {{ source('raw', 'raw_reviews') }},
LATERAL jsonb_array_elements(raw_response->'data') AS r

{% if is_incremental() %}
    WHERE (r->>'id')::BIGINT NOT IN (SELECT review_id FROM {{ this }})
{% endif %}