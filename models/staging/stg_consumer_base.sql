{{ config(
    materialized='incremental',
    unique_key='consumerid',
    incremental_strategy='merge',
    on_schema_change='sync_all_columns',
    snowflake_warehouse='WH_PIPE_COMMISSION'
) }}

with source_data as (
    select
        consumerid,
        sourcesystemtimestamp,
        is_active,
        first_name,
        last_name,
        membernumber,
        branch_member_number,
        prefix,
        suffix,
        salutation,
        account_name,
        address_line_1,
        address_line_2,
        aptsuite,
        city,
        state,
        zip,
        country,
        secondary_address_line_1,
        secondary_address_line_2,
        secondary_aptsuite,
        secondary_city,
        secondary_state,
        secondary_zip,
        secondary_country,
        secondary_address_from_month,
        secondary_address_to_month,
        primary_email,
        phone_number,
        business_phone,
        cell_phone,
        account_id,
        preferred_language,
        fax_phone,
        sourcesystem,
        primary_agent_id,
        data_source_id,
        create_date,
        isdeleted,
        deleted_timestamp
    from {{ source('consumer', 'stream_consumer') }}
    where metadata$action = 'INSERT'
    
    {% if is_incremental() %}
        and sourcesystemtimestamp > (select max(sourcesystemtimestamp) from {{ this }})
    {% endif %}
),

transformed as (
    select
        consumerid,
        sourcesystemtimestamp,
        is_active,
        first_name,
        last_name,
        membernumber,
        branch_member_number,
        prefix,
        suffix,
        salutation,
        account_name,
        -- Map address fields to raw columns as per original logic
        address_line_1 as raw_address_line_1,
        address_line_2 as raw_address_line_2,
        aptsuite as raw_aptsuite,
        city as raw_city,
        state as raw_state,
        zip as raw_zip,
        country as raw_country,
        secondary_address_line_1 as raw_secondary_address_line_1,
        secondary_address_line_2 as raw_secondary_address_line_2,
        secondary_aptsuite as raw_secondary_aptsuite,
        secondary_city as raw_secondary_city,
        secondary_state as raw_secondary_state,
        secondary_zip as raw_secondary_zip,
        secondary_country as raw_secondary_country,
        secondary_address_from_month,
        secondary_address_to_month,
        primary_email,
        phone_number,
        business_phone,
        cell_phone,
        account_id,
        preferred_language,
        fax_phone,
        sourcesystem,
        primary_agent_id,
        data_source_id,
        create_date,
        isdeleted,
        deleted_timestamp,
        current_timestamp() as updated_timestamp,
        current_timestamp() as inserted_timestamp,
        -- Add stream metadata
        'stream_consumer' as source_stream
    from source_data
)

select * from transformed 