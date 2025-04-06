{{
    config(
        materialized='table',
        tags=['merchants', 'silver']
    )
}}

with stg_merchants as (
    select * from {{ ref('stg_merchants') }}
),

final as (
    select
        -- Generate a surrogate key
        {{ dbt_utils.generate_surrogate_key(['merchant_id']) }} as merchant_sk,
        
        -- Source columns
        merchant_id,
        merchant_name,
        category,
        country,
        integration_type,
        onboarding_date,
        status,
        
        -- Add calculated columns
        datediff('day', onboarding_date, current_date()) as days_since_onboarding,
        
        -- Add metadata
        _etl_extracted_at as source_extracted_at,
        current_timestamp as dbt_updated_at
    from stg_merchants
)

select * from final