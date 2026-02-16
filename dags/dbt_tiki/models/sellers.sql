{{ config(
    materialized='table',
    post_hook=[
      'ALTER TABLE {{ this }} ADD PRIMARY KEY (seller_id)'
    ]
) }}

with cte as (
    SELECT 
        (raw_response->'data'->'seller'->>'id')::INT AS seller_id,
        (raw_response->'data'->'seller'->>'store_id')::INT AS store_id,
        raw_response->'data'->'seller'->>'name' AS name,
        raw_response->'data'->'seller'->>'url' AS url,
        raw_response->'data'->'seller'->>'icon' AS logo,
        (raw_response->'data'->'seller'->>'avg_rating_point')::NUMERIC(3, 2) AS avg_rating_point,
        (raw_response->'data'->'seller'->>'review_count')::INTEGER AS review_count,
        (raw_response->'data'->'seller'->>'total_follower')::INTEGER AS total_follower,
        (raw_response->'data'->'seller'->>'is_official')::BOOLEAN AS is_official,
        (raw_response->'data'->'seller'->>'days_since_joined')::INTEGER AS days_since_joined,
        extract_time
    FROM {{ source('raw', 'raw_sellers') }}
)
select * from cte
where seller_id != 0