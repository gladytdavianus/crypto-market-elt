with source as (
    select coin_id, symbol, name
    from {{ ref('stg_coins_list') }}
),

deduped as (
    select *
    from source
    qualify row_number() over (partition by coin_id order by coin_id) = 1
)

select * from deduped
