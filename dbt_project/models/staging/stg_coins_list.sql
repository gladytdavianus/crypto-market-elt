with source as (
    select payload
    from {{ source('crypto_raw', 'coins_list_raw') }}
),

unnested as (
    select
        json_extract_scalar(coin, '$.id') as coin_id,
        json_extract_scalar(coin, '$.symbol') as symbol,
        json_extract_scalar(coin, '$.name') as name
    from source,
    unnest(json_extract_array(payload)) as coin
)

select * from unnested
