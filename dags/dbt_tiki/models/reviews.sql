{{ config(
    materialized='incremental',
    unique_key='review_id',
    on_schema_change='sync_all_columns',
    event_time = 'extract_time',
    batch_size = 'hour',
    lookback = 1
) }}
SELECT DISTINCT ON ((r->>'id')::BIGINT)
    (r->>'id')::BIGINT                                          AS review_id,
    (r->>'product_id')::BIGINT                                  AS product_id,
    (r->>'spid')::BIGINT                                        AS spid,
    (r->'seller'->>'id')::INT                                   AS seller_id,
    (r->'created_by'->>'id')::BIGINT                            AS user_id,
    (r->'created_by'->>'name')                                  AS user_name,
    (r->>'rating')::INT                                         AS rating,
    (r->>'title')                                               AS title,
    (r->>'content')                                             AS content,
    to_timestamp(NULLIF(r->'created_by'->>'purchased_at', '')::BIGINT) AS purchased_at,
    (r->'product_attributes'->>0)                               AS variant,
    (r->>'thank_count')::INT                                    AS thank_count
FROM raw.raw_reviews,
LATERAL jsonb_array_elements(raw_response->'data') AS r
WHERE TRUE
AND extract_time >= current_date
AND (r->'seller'->>'id')::INT IS NOT NULL