with source as (
    select coin_id, payload
    from {{ source('crypto_raw', 'historical_prices_raw') }}
),

prices as (
    select
        coin_id,
        idx,
        cast(json_extract_scalar(point, '$[0]') as int64) as ts_epoch_ms,
        cast(json_extract_scalar(point, '$[1]') as float64) as price_usd
    from source,
    unnest(json_extract_array(payload, '$.prices')) as point with offset idx
),

market_caps as (
    select
        coin_id,
        idx,
        cast(json_extract_scalar(point, '$[1]') as float64) as market_cap_usd
    from source,
    unnest(json_extract_array(payload, '$.market_caps')) as point with offset idx
),

volumes as (
    select
        coin_id,
        idx,
        cast(json_extract_scalar(point, '$[1]') as float64) as total_volume_usd
    from source,
    unnest(json_extract_array(payload, '$.total_volumes')) as point with offset idx
)

select
    p.coin_id,
    timestamp_millis(p.ts_epoch_ms) as price_date,
    p.price_usd,
    m.market_cap_usd,
    v.total_volume_usd
from prices p
left join market_caps m using (coin_id, idx)
left join volumes v using (coin_id, idx)
