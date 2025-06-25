{{ config(
    materialized='incremental',
    unique_key='consumerid',
    incremental_strategy='merge',
    on_schema_change='sync_all_columns',
    snowflake_warehouse='WH_PIPE_COMMISSION'
) }}

with customer_names as (
    select *
    from {{ ref('stg_consumer_real_person_validation') }}
    {% if is_incremental() %}
        where updated_timestamp > (select max(updated_timestamp) from {{ this }})
    {% endif %}
),

-- Get current timestamp for date calculations
current_time_lookup as (
    select currenttimestamp
    from {{ source('sharedservices_config', 'lookup_current_timestamp') }}
),

-- Booking data for travel pattern analysis
booking_data as (
    select
        consumer_id,
        datediff(day, tripstartdate, tripenddate) + 1 as trip_days,
        year(tripstartdate) as trip_year,
        tripbookingcount
    from {{ source('published', 'booking_master') }} pbm
    cross join current_time_lookup l
    left join {{ source('published', 'data_source_core_extension') }} dsce
        on pbm.data_source_id = dsce.data_source_id
    where customertripnumber is not null
        and year(tripstartdate) >= year(cast(l.currenttimestamp as date)) - 3
        and dsce.is_data_source_historic = 0
        and pbm.void_transaction = false
        and pbm.exclude_booking = false
        and pbm.is_deleted = false
    group by consumer_id, trip_days, trip_year, tripbookingcount
),

-- Aggregate trip data by consumer and year
trip_aggregates as (
    select
        consumer_id,
        trip_year,
        sum(trip_days) as traveled_days_per_yr,
        sum(tripbookingcount) as booking_count_per_yr
    from booking_data
    group by consumer_id, trip_year
),

-- Identify consumers with excessive travel patterns
travel_pattern_analysis as (
    select
        consumer_id,
        min(
            case 
                when booking_count_per_yr > 400 or traveled_days_per_yr > 350 
                then 0 
                else 1 
            end
        ) as is_real_person_travel
    from trip_aggregates
    group by consumer_id
),

-- Stage 1: Corporate customers (based on travel type)
corporate_customers as (
    select
        c.consumerid,
        0 as is_real_person,
        'corporate' as classification_reason,
        'Customer marked as corporate travel type' as classification_description
    from customer_names c
    join {{ source('consumer', 'additional_attributes') }} ca 
        on c.consumerid = ca.consumerid
    where upper(ca.customertraveltype) = 'CORPORATE'
),

-- Stage 2: Name-based pattern filtering
name_based_filter as (
    select
        c.consumerid,
        0 as is_real_person,
        'name_pattern' as classification_reason,
        'Name contains corporate/business indicators' as classification_description
    from customer_names c
    left join corporate_customers co on c.consumerid = co.consumerid
    where co.consumerid is null
        and (
            charindex('acct', c.fullname) > 0 or
            charindex('corpo', c.fullname) > 0 or
            charindex('co.', c.fullname) > 0 or
            charindex('corp.', c.fullname) > 0 or
            charindex('corp ', c.fullname) > 0 or
            c.fullname ilike '%corp' or
            charindex('llc', c.fullname) > 0 or
            charindex('office', c.fullname) > 0 or
            charindex('account', c.fullname) > 0 or
            charindex('client', c.fullname) > 0 or
            charindex('commis', c.fullname) > 0 or
            charindex('unknown', c.fullname) > 0 or
            charindex('marketing', c.fullname) > 0 or
            charindex('misc ', c.fullname) > 0 or
            charindex('misc.', c.fullname) > 0 or
            charindex('misce', c.fullname) > 0 or
            c.fullname ilike '%misc' or
            charindex('travel', c.fullname) > 0 or
            charindex('leisure', c.fullname) > 0 or
            charindex('inc.', c.fullname) > 0 or
            c.fullname ilike '% inc' or
            c.fullname ilike '% group' or
            charindex('ltd', c.fullname) > 0 or
            charindex('limited', c.fullname) > 0 or
            charindex('svcs', c.fullname) > 0 or
            charindex('service', c.fullname) > 0 or
            charindex('house-', c.fullname) > 0 or
            charindex('house -', c.fullname) > 0 or
            charindex('walk-in', c.fullname) > 0 or
            charindex('company', c.fullname) > 0 or
            charindex('entertainment', c.fullname) > 0 or
            charindex('consulting', c.fullname) > 0 or
            charindex('foundation', c.fullname) > 0 or
            charindex('construction', c.fullname) > 0 or
            charindex('corps', c.fullname) > 0 or
            charindex('school', c.fullname) > 0 or
            charindex('university', c.fullname) > 0 or
            charindex('association', c.fullname) > 0 or
            charindex('society', c.fullname) > 0 or
            charindex('academy', c.fullname) > 0 or
            charindex('acadamy', c.fullname) > 0 or
            charindex('network', c.fullname) > 0 or
            charindex('community', c.fullname) > 0 or
            charindex('dept', c.fullname) > 0 or
            charindex('department', c.fullname) > 0 or
            charindex('consul', c.fullname) > 0 or
            charindex('council', c.fullname) > 0 or
            charindex('industr', c.fullname) > 0 or
            charindex('techno', c.fullname) > 0 or
            c.fullname ilike '%house%ac%' or
            c.fullname ilike '%house%walk%' or
            c.fullname is null
        )
),

-- Combine corporate and name-based exclusions
corporate_name_based_customers as (
    select consumerid from corporate_customers
    union all
    select consumerid from name_based_filter
),

-- Stage 3: Number-based pattern filtering
number_based_filter as (
    select
        c.consumerid,
        0 as is_real_person,
        'number_pattern' as classification_reason,
        'Name contains multiple numbers indicating business account' as classification_description
    from customer_names c
    left join corporate_name_based_customers co on c.consumerid = co.consumerid
    where co.consumerid is null
        and regexp_like(c.fullname, '.*[0-9].*.*[0-9].*') = true
),

-- Combine corporate, name-based, and number-based exclusions
corporate_name_number_based_customers as (
    select consumerid from corporate_customers
    union all
    select consumerid from name_based_filter
    union all
    select consumerid from number_based_filter
),

-- Stage 4: Travel pattern-based filtering
travel_pattern_filter as (
    select
        c.consumerid,
        0 as is_real_person,
        'travel_pattern' as classification_reason,
        'Excessive travel pattern (>400 bookings or >350 travel days per year)' as classification_description
    from customer_names c
    join travel_pattern_analysis r on r.consumer_id = c.consumerid
    left join corporate_name_number_based_customers rpt on c.consumerid = rpt.consumerid
    where r.is_real_person_travel = 0
        and rpt.consumerid is null
),

-- Combine all exclusion categories
all_excluded_customers as (
    select consumerid from corporate_customers
    union all
    select consumerid from name_based_filter
    union all
    select consumerid from number_based_filter
    union all
    select consumerid from travel_pattern_filter
),

-- Stage 5: Remaining customers are classified as real persons
remaining_real_persons as (
    select
        a.consumerid,
        1 as is_real_person,
        'real_person' as classification_reason,
        'Passed all validation checks' as classification_description
    from customer_names a
    left join all_excluded_customers b on a.consumerid = b.consumerid
    where b.consumerid is null
),

-- Final consolidated results
final_classification as (
    select
        consumerid,
        is_real_person,
        classification_reason,
        classification_description
    from corporate_customers

    union all

    select
        consumerid,
        is_real_person,
        classification_reason,
        classification_description
    from name_based_filter

    union all

    select
        consumerid,
        is_real_person,
        classification_reason,
        classification_description
    from number_based_filter

    union all

    select
        consumerid,
        is_real_person,
        classification_reason,
        classification_description
    from travel_pattern_filter

    union all

    select
        consumerid,
        is_real_person,
        classification_reason,
        classification_description
    from remaining_real_persons
),

-- Add metadata and timestamps
enriched_results as (
    select
        fc.consumerid,
        fc.is_real_person,
        fc.classification_reason,
        fc.classification_description,
        cn.fullname,
        cn.first_name,
        cn.last_name,
        cn.account_name,
        current_timestamp() as processed_at,
        current_timestamp() as inserted_datetime,
        current_timestamp() as updated_datetime,
        -- Add data quality metrics
        case 
            when fc.classification_reason = 'corporate' then 1
            when fc.classification_reason = 'name_pattern' then 2
            when fc.classification_reason = 'number_pattern' then 3
            when fc.classification_reason = 'travel_pattern' then 4
            when fc.classification_reason = 'real_person' then 5
        end as classification_stage,
        -- Track source streams used
        array_construct('stream_isrealperson', 'additional_attributes', 'booking_master') as source_streams
    from final_classification fc
    left join customer_names cn on fc.consumerid = cn.consumerid
)

select * from enriched_results 