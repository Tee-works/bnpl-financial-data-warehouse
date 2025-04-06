with source as (
    select * from {{ source('bronze', 'customers') }}
),

renamed as (
    select
        customer_id,
        email,
        first_name,
        last_name,
        phone_number,
        country,
        city,
        registration_date,
        last_login_date,
        status,
        _etl_extracted_at
    from source
)

select * from renamed