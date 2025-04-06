with source as (
    select * from {{ source('bronze', 'payment_plans') }}
),

renamed as (
    select
        plan_id,
        transaction_id,
        customer_id,
        merchant_id,
        plan_date,
        total_amount,
        installment_count,
        first_installment_amount,
        status,
        _etl_extracted_at
    from source
)

select * from renamed