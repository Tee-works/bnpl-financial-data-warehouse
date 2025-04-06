with source as (
    select * from {{ source('bronze', 'merchants') }}
),

renamed as (
    select
        merchant_id,
        merchant_name,
        category,
        country,
        integration_type,
        onboarding_date,
        status,
        _etl_extracted_at
    from source
)

select * from renamed