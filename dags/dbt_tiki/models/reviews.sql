{{ config(
    materialized='table',
    post_hook=[
      'ALTER TABLE {{ this }} ADD PRIMARY KEY (review_id)'
    ]
) }}

SELECT 
    (r->>'id')::BIGINT as review_id,
    (r->>'product_id')::BIGINT as product_id,
    (r->>'spid')::BIGINT as spid,
    (r->'seller'->>'id')::INT as seller_id,
    (r->'created_by'->>'id')::BIGINT as user_id,
    (r->'created_by'->>'name') as user_name,
    (r->>'rating')::INT as rating,
    (r->>'title') as title,
    (r->>'content') as content,
    to_timestamp((r->'created_by'->>'purchased_at')::BIGINT) as purchased_at,
    (r->'product_attributes'->>0) as variant, -- Lấy thông tin variant (Tập 1, Tập 2...)
    (r->>'thank_count')::INT as thank_count
FROM {{ source('raw', 'raw_reviews') }}, lateral jsonb_array_elements(raw_response->'data') AS r

-- image
-- SELECT
--     (r->>'id')::BIGINT as review_id,
--     (img->>'id')::BIGINT as img_id,
--     (img->>'full_path') as img_url
-- FROM raw_reviews, 
--      jsonb_array_elements(raw_response->'data') AS r,
--      jsonb_array_elements(r->'images') AS img