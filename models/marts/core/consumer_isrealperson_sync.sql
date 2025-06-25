{{ config(
    materialized='incremental',
    unique_key='consumerid',
    incremental_strategy='merge',
    on_schema_change='sync_all_columns',
    snowflake_warehouse='WH_PIPE_COMMISSION'
) }}

with real_person_validation as (
    select *
    from {{ ref('dim_consumer_real_person_validation') }}
    {% if is_incremental() %}
        where updated_datetime > (select max(updated_datetime) from {{ this }})
    {% endif %}
),

-- Sync format to match original consumer_isrealperson table structure
sync_format as (
    select
        consumerid,
        is_real_person as isrealperson,  -- Map to original column name
        inserted_datetime,
        updated_datetime,
        -- Additional metadata for troubleshooting
        classification_reason,
        classification_description,
        classification_stage
    from real_person_validation
)

select * from sync_format 