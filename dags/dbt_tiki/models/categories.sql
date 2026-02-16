{{ config(
    materialized='table',
    post_hook=[
      'ALTER TABLE {{ this }} ADD PRIMARY KEY (category_id)'
    ]
) }}

select 
    (value->>'id')::int as category_id,
    (value->'parent_id')::int as parent_id,
    value->>'name' as name,
    value->>'url_key' as url_key,
    value->>'url_path' as url_path,
    (value->>'level')::int as level,
    value->>'status' as status,
    (value->>'is_leaf')::boolean as is_leaf, 
    (value->>'product_count')::int as product_count,
    value->>'thumbnail_url' as thumbnail_url,
    value->>'meta_title' as meta_title,
    value->>'meta_description' as meta_description
from {{ source('raw', 'raw_categories') }},
lateral jsonb_array_elements(raw_response) as t
where raw_response::text != '[]'
order by category_id desc