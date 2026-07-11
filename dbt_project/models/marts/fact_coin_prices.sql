with from_historical as (
    select
        coin_id,
        date(price_date) as price_date,
        price_usd,
        market_cap_usd,
        total_volume_usd,
        'historical_backfill' as source
    from {{ ref('stg_historical_prices') }}
),

from_daily_snapshot as (
    select
        coin_id,
        date(last_updated) as price_date,
        current_price as price_usd,
        market_cap as market_cap_usd,
        total_volume as total_volume_usd,
        'daily_snapshot' as source
    from {{ ref('stg_market_data') }}
),

combined as (
    select * from from_historical
    union all
    select * from from_daily_snapshot
),

deduped as (
    select *
    from combined
    qualify row_number() over (
        partition by coin_id, price_date
        order by case when source = 'daily_snapshot' then 1 else 2 end
    ) = 1
)

select
    coin_id,
    price_date,
    price_usd,
    market_cap_usd,
    total_volume_usd
from deduped
