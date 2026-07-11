with source as (
    select payload, ingested_at
    from {{ source('crypto_raw', 'market_data_raw') }}
),

latest_snapshot as (
    select payload
    from source
    qualify row_number() over (order by ingested_at desc) = 1
),

unnested as (
    select
        json_extract_scalar(coin, '$.id') as coin_id,
        json_extract_scalar(coin, '$.symbol') as symbol,
        json_extract_scalar(coin, '$.name') as name,
        cast(json_extract_scalar(coin, '$.current_price') as float64) as current_price,
        cast(json_extract_scalar(coin, '$.market_cap') as float64) as market_cap,
        cast(json_extract_scalar(coin, '$.market_cap_rank') as int64) as market_cap_rank,
        cast(json_extract_scalar(coin, '$.total_volume') as float64) as total_volume,
        cast(json_extract_scalar(coin, '$.high_24h') as float64) as high_24h,
        cast(json_extract_scalar(coin, '$.low_24h') as float64) as low_24h,
        cast(json_extract_scalar(coin, '$.price_change_24h') as float64) as price_change_24h,
        cast(json_extract_scalar(coin, '$.price_change_percentage_24h') as float64) as price_change_percentage_24h,
        cast(json_extract_scalar(coin, '$.circulating_supply') as float64) as circulating_supply,
        cast(json_extract_scalar(coin, '$.total_supply') as float64) as total_supply,
        cast(json_extract_scalar(coin, '$.max_supply') as float64) as max_supply,
        cast(json_extract_scalar(coin, '$.ath') as float64) as ath,
        cast(json_extract_scalar(coin, '$.atl') as float64) as atl,
        cast(json_extract_scalar(coin, '$.last_updated') as timestamp) as last_updated
    from latest_snapshot,
    unnest(json_extract_array(payload)) as coin
)

select * from unnested
