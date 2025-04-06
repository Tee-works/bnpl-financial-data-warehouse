{{
    config(
        materialized='table',
        tags=['customers', 'silver']
    )
}}

with stg_user_events as (
    select * from {{ ref('stg_user_events') }}
),

dim_customers as (
    select * from {{ ref('dim_customers') }}
),

dim_merchants as (
    select * from {{ ref('dim_merchants') }}
),

events_with_sk as (
    select
        e.event_id,
        c.customer_sk,
        m.merchant_sk,
        e.event_timestamp,
        cast(e.event_timestamp as date) as event_date_key,

        e.event_type,
        e.platform,
        e.session_id,
        e.device_type,
        e.country,
        e._etl_extracted_at
    from stg_user_events e
    left join dim_customers c on e.customer_id = c.customer_id
    left join dim_merchants m on e.merchant_id = m.merchant_id
),

final as (
    select
        -- Generate a surrogate key
        {{ dbt_utils.generate_surrogate_key(['event_id']) }} as event_sk,
        
        -- Natural and foreign keys
        event_id,
        customer_sk,
        merchant_sk,
        event_date_key,
        
        -- Event details
        event_timestamp,
        event_type,
        platform,
        session_id,
        device_type,
        country,
        
        -- Add metadata
        _etl_extracted_at as source_extracted_at,
        current_timestamp as dbt_updated_at
    from events_with_sk
)

select * from final