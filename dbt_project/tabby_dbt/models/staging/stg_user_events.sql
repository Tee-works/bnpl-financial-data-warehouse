with source as (
    select * from {{ source('bronze', 'user_events') }}
),

renamed as (
    select
        event_id,
        event_timestamp,
        customer_id,
        event_type,
        platform,
        merchant_id,
        session_id,
        device_type,
        country,
        _etl_extracted_at
    from source
)

select * from renamed