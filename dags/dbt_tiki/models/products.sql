{{ config(
    materialized='incremental',
    unique_key='spid',
    on_schema_change='sync_all_columns',
    pre_hook=[
        "ALTER TABLE IF EXISTS {{ this }} DROP CONSTRAINT IF EXISTS fk_product_seller"
    ],
    post_hook=[
        """
        DO $$ BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint c
                JOIN pg_class t     ON t.oid = c.conrelid
                JOIN pg_namespace n ON n.oid = t.relnamespace
                WHERE c.contype = 'p'
                  AND t.relname  = 'products'
                  AND n.nspname  = 'cleaned'
            ) THEN
                ALTER TABLE {{ this }} ADD PRIMARY KEY (spid);
            END IF;
        END $$;
        """,
        """
        DO $$ BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint c
                JOIN pg_class t     ON t.oid = c.conrelid
                JOIN pg_namespace n ON n.oid = t.relnamespace
                WHERE c.conname   = 'fk_product_seller'
                  AND t.relname   = 'products'
                  AND n.nspname   = 'cleaned'
            ) THEN
                ALTER TABLE {{ this }}
                ADD CONSTRAINT fk_product_seller
                FOREIGN KEY (seller_id) REFERENCES {{ ref('sellers') }} (seller_id)
                ON DELETE RESTRICT;
            END IF;
        END $$;
        """
    ]
) }}

WITH raw_items AS (
    SELECT
        (item->>'id')::INT                                                              AS product_id,
        (item->>'seller_product_id')::BIGINT                                            AS spid,
        item->>'name'                                                                   AS name,
        (item->>'sku')::BIGINT                                                          AS sku,
        (item->'seller_id')::BIGINT                                                     AS seller_id,
        item->>'url_key'                                                                AS url_key,
        item->>'url_path'                                                               AS url_path,
        item->>'short_description'                                                      AS short_description,
        (item->>'price')::NUMERIC                                                       AS price,
        (item->>'list_price')::NUMERIC                                                  AS list_price,
        (item->>'original_price')::NUMERIC                                              AS original_price,
        (item->>'discount')::NUMERIC                                                    AS discount,
        (item->>'discount_rate')::NUMERIC                                               AS discount_rate,
        (item->>'rating_average')::NUMERIC                                              AS rating_average,
        (item->>'review_count')::INTEGER                                                AS review_count,
        (item->>'order_count')::INTEGER                                                 AS order_count,
        (item->>'favourite_count')::INTEGER                                             AS favourite_count,
        item->>'thumbnail_url'                                                          AS thumbnail_url,
        (item->>'has_ebook')::BOOLEAN                                                   AS has_ebook,
        item->>'inventory_status'                                                       AS inventory_status,
        item->>'productset_group_name'                                                  AS productset_group_name,
        (item->'visible_impression_info'->'amplitude'->>'all_time_quantity_sold')::INT  AS all_time_quantity_sold,
        NULL::TEXT                                                                      AS meta_title,
        NULL::TEXT                                                                      AS meta_description,
        CURRENT_TIMESTAMP                                                               AS last_updated,
        extract_time
    FROM {{ source('raw', 'raw_product_listings') }},
    LATERAL jsonb_array_elements(raw_response->'data') AS item

    {% if is_incremental() %}
        WHERE extract_time > (SELECT MAX(last_updated) FROM {{ this }})
    {% endif %}
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY spid
            ORDER BY extract_time DESC
        ) AS rn
    FROM raw_items
)

SELECT
    product_id, spid, name, sku, seller_id,
    url_key, url_path, short_description,
    price, list_price, original_price, discount, discount_rate,
    rating_average, review_count, order_count, favourite_count,
    thumbnail_url, has_ebook, inventory_status, productset_group_name,
    all_time_quantity_sold, meta_title, meta_description, last_updated
FROM deduplicated
WHERE rn = 1