{{ config(
    materialized='incremental',
    unique_key='consumerid',
    incremental_strategy='merge',
    on_schema_change='sync_all_columns',
    snowflake_warehouse='WH_PIPE_COMMISSION'
) }}

with base_consumer as (
    select *
    from {{ ref('stg_consumer_base') }}
    {% if is_incremental() %}
        where updated_timestamp > (select max(updated_timestamp) from {{ this }})
    {% endif %}
),

consumer_extension as (
    select *
    from {{ ref('stg_consumer_extension') }}
    {% if is_incremental() %}
        where updated_timestamp > (select max(updated_timestamp) from {{ this }})
    {% endif %}
),

consumer_append_attributes as (
    select *
    from {{ ref('stg_consumer_append_attributes') }}
    {% if is_incremental() %}
        where updated_timestamp > (select max(updated_timestamp) from {{ this }})
    {% endif %}
),

-- CRM Consumer Stream
crm_consumer as (
    select
        consumer_id as consumerid,
        crm_consumer_id,
        current_timestamp() as updated_timestamp
    from {{ source('consumer', 'stream_crm_consumer') }}
    where metadata$action = 'INSERT'
    {% if is_incremental() %}
        and consumer_id in (
            select distinct consumer_id 
            from {{ source('consumer', 'stream_crm_consumer') }}
            where metadata$action = 'INSERT'
        )
    {% endif %}
),

-- Consumer IsRealPerson Stream
consumer_isrealperson as (
    select
        consumerid,
        isrealperson,
        current_timestamp() as updated_timestamp
    from {{ source('consumer', 'stream_consumer_isrealperson') }}
    where metadata$action = 'INSERT'
    {% if is_incremental() %}
        and consumerid in (
            select distinct consumerid 
            from {{ source('consumer', 'stream_consumer_isrealperson') }}
            where metadata$action = 'INSERT'
        )
    {% endif %}
),

-- Seasonal Address Selector Stream
seasonal_address as (
    select
        consumerid,
        isseasonalflag,
        current_timestamp() as updated_timestamp
    from {{ source('consumer', 'stream_consumer_seasonal_address_selector') }}
    where metadata$action = 'INSERT'
    {% if is_incremental() %}
        and consumerid in (
            select distinct consumerid 
            from {{ source('consumer', 'stream_consumer_seasonal_address_selector') }}
            where metadata$action = 'INSERT'
        )
    {% endif %}
),

-- Email Marketing Override Stream
email_marketing_override as (
    select
        consumerid,
        email_marketing_override,
        current_timestamp() as updated_timestamp
    from {{ source('consumer', 'stream_consumer_email_marketing_override') }}
    where metadata$action = 'INSERT'
    {% if is_incremental() %}
        and consumerid in (
            select distinct consumerid 
            from {{ source('consumer', 'stream_consumer_email_marketing_override') }}
            where metadata$action = 'INSERT'
        )
    {% endif %}
),

-- Special Dates Stream
special_dates as (
    select
        consumerid,
        anniversary,
        birthday,
        wedding,
        current_timestamp() as updated_timestamp
    from {{ source('consumer', 'stream_special_dates') }}
    where metadata$action = 'INSERT'
    {% if is_incremental() %}
        and consumerid in (
            select distinct consumerid 
            from {{ source('consumer', 'stream_special_dates') }}
            where metadata$action = 'INSERT'
        )
    {% endif %}
),

-- Additional Attributes Stream
additional_attributes as (
    select
        consumerid,
        gender,
        citizenship,
        issmoker,
        customertraveltype,
        ismarketable,
        current_timestamp() as updated_timestamp
    from {{ source('consumer', 'stream_additional_attributes') }}
    where metadata$action = 'INSERT'
    {% if is_incremental() %}
        and consumerid in (
            select distinct consumerid 
            from {{ source('consumer', 'stream_additional_attributes') }}
            where metadata$action = 'INSERT'
        )
    {% endif %}
),

-- Travel Segment Stream (with consumer join)
travel_segment as (
    select
        ts.consumerid,
        ts.tourgroups,
        ts.tourhostedtourssetdepartures,
        ts.tourcustomexperiencefit,
        ts.tourchartersincentive,
        ts.tourspecialtycruise,
        ts.tourvacationpackages,
        ts.tourtravelservices,
        ts.touractive,
        ts.tourspend12months,
        ts.tourspend12months0to10k,
        ts.tourspend12months10kto25k,
        ts.tourspend12months25kto50k,
        ts.tourspend12months50kplus,
        ts.tourspend12months0,
        ts.tourhistory1tour,
        ts.tourhistory2to5tour,
        ts.tourhistory6plustours,
        ts.tourhistory0tour,
        ts.tourpending,
        ts.tourhistory,
        current_timestamp() as updated_timestamp
    from {{ source('consumer', 'stream_travel_segment') }} ts
    join {{ source('consumer', 'consumer') }} c 
        on ts.consumerid = c.consumerid
    where ts.metadata$action = 'INSERT'
    {% if is_incremental() %}
        and ts.consumerid in (
            select distinct consumerid 
            from {{ source('consumer', 'stream_travel_segment') }}
            where metadata$action = 'INSERT'
        )
    {% endif %}
),

-- Hotel Travel Segment Stream (with consumer join)
hotel_travel_segment as (
    select
        hts.consumerid,
        hts.hotelexperiencebeach,
        hts.hotelexperienceecotourism,
        hts.hotelexperiencelandmarks,
        hts.hotelexperiencecitylife,
        hts.hotelexperiencegolf,
        hts.hotelexperienceski,
        hts.hotelexperienceseclusion,
        hts.hotelexperiencelocalimmersion,
        hts.hotelexperiencewellness,
        hts.hotelexperienceadventure,
        hts.hotelroomstyleeclectic,
        hts.hotelroomstyleindigenous,
        hts.hotelroomstyleclassic,
        hts.hotelroomstylecontemporary,
        hts.hotelvibecasual,
        hts.hotelvibesophisticated,
        hts.hotelvibezen,
        hts.hotelvibehip,
        hts.hotelpending,
        hts.hotelspend12months,
        hts.hotelhistory,
        current_timestamp() as updated_timestamp
    from {{ source('consumer', 'stream_hotel_travel_segment') }} hts
    join {{ source('consumer', 'consumer') }} c 
        on hts.consumerid = c.consumerid
    where hts.metadata$action = 'INSERT'
    {% if is_incremental() %}
        and hts.consumerid in (
            select distinct consumerid 
            from {{ source('consumer', 'stream_hotel_travel_segment') }}
            where metadata$action = 'INSERT'
        )
    {% endif %}
),

-- Profile Segment Stream (with consumer join)
profile_segment as (
    select
        p.consumer_id as consumerid,
        p.clientsegment,
        current_timestamp() as updated_timestamp
    from {{ source('consumer', 'stream_profile_segment') }} p
    join {{ source('consumer', 'consumer') }} c 
        on p.consumer_id = c.consumerid
    where p.metadata$action = 'INSERT'
    {% if is_incremental() %}
        and p.consumer_id in (
            select distinct consumer_id 
            from {{ source('consumer', 'stream_profile_segment') }}
            where metadata$action = 'INSERT'
        )
    {% endif %}
),

-- Profile Stream (detailed preferences)
profile_preferences as (
    select
        consumerid,
        totaltripbudget_3000_8000,
        totaltripbudget_8000_15000,
        totaltripbudget_upto3000,
        totaltripbudget_15000plus,
        beddingtype_tempurpedic,
        beddingtype_tempurpedicclone,
        cartype_full_size,
        classofservice_businessclass,
        classofservice_firstclass,
        clienttype_client,
        clienttype_referral,
        clienttype_vip,
        -- Destinations (sample - would include all destination fields)
        destinations_alaska,
        destinations_antarctica,
        destinations_arctic,
        destinations_australia,
        destinations_hawaii,
        destinations_europe,
        destinations_france,
        destinations_germany,
        -- Interests (sample - would include all interest fields)
        interests_adventure,
        interests_climbing,
        interests_snowsports,
        -- Lifecycle
        lifecycle_retired,
        lifecycle_married,
        lifecycle_single,
        -- Marketing preferences
        marketing_directmail,
        marketing_emailmarketing,
        current_timestamp() as updated_timestamp
    from {{ source('consumer', 'stream_profile') }}
    where metadata$action = 'INSERT'
    {% if is_incremental() %}
        and consumerid in (
            select distinct consumerid 
            from {{ source('consumer', 'stream_profile') }}
            where metadata$action = 'INSERT'
        )
    {% endif %}
),

-- Address Normalized Stream
address_normalized as (
    select
        consumerid,
        normalized_primary_address_1,
        normalized_primary_address_2,
        normalized_primary_apt_suite,
        normalized_primary_zip,
        normalized_primary_country,
        normalized_primary_state,
        normalized_primary_city,
        normalized_primary_region,
        normalized_secondary_address_1,
        normalized_secondary_address_2,
        normalized_secondary_apt_suite,
        normalized_secondary_zip,
        normalized_secondary_country,
        normalized_secondary_state,
        normalized_secondary_city,
        normalized_secondary_region,
        primary_geocode_id,
        secondary_geocode_id,
        current_timestamp() as updated_timestamp
    from {{ source('consumer', 'stream_address_normalized') }}
    where metadata$action = 'INSERT'
    {% if is_incremental() %}
        and consumerid in (
            select distinct consumerid 
            from {{ source('consumer', 'stream_address_normalized') }}
            where metadata$action = 'INSERT'
        )
    {% endif %}
),

-- Email Validation Stream
email_validation as (
    select
        consumerid,
        is_primary_email_valid,
        verified_primary_email,
        current_timestamp() as updated_timestamp
    from {{ source('consumer', 'stream_email_validation') }}
    where metadata$action = 'INSERT'
    {% if is_incremental() %}
        and consumerid in (
            select distinct consumerid 
            from {{ source('consumer', 'stream_email_validation') }}
            where metadata$action = 'INSERT'
        )
    {% endif %}
),

-- Agency Advisor Map Stream
agency_advisor_map as (
    select
        consumerid,
        agencyid,
        agencybranchid,
        primary_advisor_id,
        marketing_advisor_id,
        marketing_agency_branch_id,
        is_agency_active,
        current_timestamp() as updated_timestamp
    from {{ source('consumer', 'stream_agency_advisor_map') }}
    where metadata$action = 'INSERT'
    {% if is_incremental() %}
        and consumerid in (
            select distinct consumerid 
            from {{ source('consumer', 'stream_agency_advisor_map') }}
            where metadata$action = 'INSERT'
        )
    {% endif %}
),

-- Consumer Excluded Stream
consumer_excluded as (
    select
        consumerid,
        coalesce(is_excluded, false) as is_excluded,
        exclude_reason,
        current_timestamp() as updated_timestamp
    from {{ source('consumer', 'stream_consumer_isexcluded') }}
    where metadata$action = 'INSERT'
    {% if is_incremental() %}
        and consumerid in (
            select distinct consumerid 
            from {{ source('consumer', 'stream_consumer_isexcluded') }}
            where metadata$action = 'INSERT'
        )
    {% endif %}
),

-- Consumer Total Spend Stream (with consumer join)
consumer_totalspend as (
    select
        t.consumerid,
        t.totalspend12months,
        current_timestamp() as updated_timestamp
    from {{ source('consumer', 'stream_consumer_totalspend') }} t
    join {{ source('consumer', 'consumer') }} c 
        on t.consumerid = c.consumerid
    where t.metadata$action = 'INSERT'
    {% if is_incremental() %}
        and t.consumerid in (
            select distinct consumerid 
            from {{ source('consumer', 'stream_consumer_totalspend') }}
            where metadata$action = 'INSERT'
        )
    {% endif %}
),

-- Consumer Soft Deleted Stream
consumer_soft_deleted as (
    select
        consumerid,
        is_soft_deleted,
        current_timestamp() as updated_timestamp
    from {{ source('consumer', 'stream_consumer_soft_deleted') }}
    where metadata$action = 'INSERT'
    {% if is_incremental() %}
        and consumerid in (
            select distinct consumerid 
            from {{ source('consumer', 'stream_consumer_soft_deleted') }}
            where metadata$action = 'INSERT'
        )
    {% endif %}
),

-- Union all consumer IDs from all streams
all_consumer_ids as (
    select consumerid from base_consumer
    union
    select consumerid from consumer_extension
    union
    select consumerid from crm_consumer
    union
    select consumerid from consumer_isrealperson
    union
    select consumerid from seasonal_address
    union
    select consumerid from email_marketing_override
    union
    select consumerid from special_dates
    union
    select consumerid from additional_attributes
    union
    select consumerid from consumer_append_attributes
    union
    select consumerid from travel_segment
    union
    select consumerid from hotel_travel_segment
    union
    select consumerid from profile_segment
    union
    select consumerid from profile_preferences
    union
    select consumerid from address_normalized
    union
    select consumerid from email_validation
    union
    select consumerid from agency_advisor_map
    union
    select consumerid from consumer_excluded
    union
    select consumerid from consumer_totalspend
    union
    select consumerid from consumer_soft_deleted
),

-- Final consolidated consumer master using FULL OUTER JOINs
final_consumer_master as (
    select
        coalesce(
            base.consumerid,
            ext.consumerid,
            crm.consumerid,
            real_person.consumerid,
            seasonal.consumerid,
            email_mkt.consumerid,
            dates.consumerid,
            addl_attr.consumerid,
            append_attr.consumerid,
            travel.consumerid,
            hotel.consumerid,
            profile_seg.consumerid,
            profile_pref.consumerid,
            addr_norm.consumerid,
            email_val.consumerid,
            agency.consumerid,
            excluded.consumerid,
            spend.consumerid,
            soft_del.consumerid
        ) as consumerid,
        
        -- Base consumer fields
        base.sourcesystemtimestamp,
        base.is_active,
        base.first_name,
        base.last_name,
        base.membernumber,
        base.branch_member_number,
        base.prefix,
        base.suffix,
        base.salutation,
        base.account_name,
        base.raw_address_line_1,
        base.raw_address_line_2,
        base.raw_aptsuite,
        base.raw_city,
        base.raw_state,
        base.raw_zip,
        base.raw_country,
        base.raw_secondary_address_line_1,
        base.raw_secondary_address_line_2,
        base.raw_secondary_aptsuite,
        base.raw_secondary_city,
        base.raw_secondary_state,
        base.raw_secondary_zip,
        base.raw_secondary_country,
        base.secondary_address_from_month,
        base.secondary_address_to_month,
        base.primary_email,
        base.phone_number,
        base.business_phone,
        base.cell_phone,
        base.account_id,
        base.preferred_language,
        base.fax_phone,
        base.sourcesystem,
        base.primary_agent_id,
        base.data_source_id,
        base.create_date,
        base.isdeleted,
        base.deleted_timestamp,
        
        -- Extension fields
        ext.interface_id,
        ext.agent_interface_id,
        ext.extended_data,
        
        -- CRM fields
        crm.crm_consumer_id,
        
        -- Real person classification
        real_person.isrealperson,
        
        -- Seasonal address
        seasonal.isseasonalflag,
        
        -- Email marketing
        email_mkt.email_marketing_override,
        
        -- Special dates
        dates.anniversary,
        dates.birthday,
        dates.wedding,
        
        -- Additional attributes
        addl_attr.gender,
        addl_attr.citizenship,
        addl_attr.issmoker,
        addl_attr.customertraveltype,
        addl_attr.ismarketable,
        
        -- Append attributes (sample - would include all 200+ fields)
        append_attr.age,
        append_attr.incomerange,
        append_attr.educationlevel,
        append_attr.homevaluerange,
        append_attr.networth,
        append_attr.maritalstatus_married,
        append_attr.maritalstatus_single,
        append_attr.numberofadultsinhh,
        append_attr.numberofchildren,
        append_attr.ownscat,
        append_attr.ownsdog,
        append_attr.religion,
        append_attr.mosaic,
        append_attr.mosaic_group,
        
        -- Travel segment
        travel.tourgroups,
        travel.tourspend12months,
        travel.tourhistory,
        
        -- Hotel segment
        hotel.hotelspend12months,
        hotel.hotelhistory,
        
        -- Profile segment
        profile_seg.clientsegment,
        
        -- Profile preferences (sample)
        profile_pref.totaltripbudget_3000_8000,
        profile_pref.classofservice_businessclass,
        profile_pref.destinations_hawaii,
        profile_pref.interests_adventure,
        profile_pref.lifecycle_retired,
        profile_pref.marketing_directmail,
        
        -- Normalized address
        addr_norm.normalized_primary_address_1,
        addr_norm.normalized_primary_city,
        addr_norm.normalized_primary_state,
        addr_norm.normalized_primary_zip,
        addr_norm.primary_geocode_id,
        
        -- Email validation
        email_val.is_primary_email_valid,
        email_val.verified_primary_email,
        
        -- Agency advisor mapping
        agency.agencyid,
        agency.agencybranchid,
        agency.primary_advisor_id,
        agency.marketing_advisor_id,
        agency.is_agency_active,
        
        -- Exclusion
        excluded.is_excluded,
        excluded.exclude_reason,
        
        -- Total spend
        spend.totalspend12months,
        
        -- Soft deletion
        soft_del.is_soft_deleted,
        
        -- Metadata
        current_timestamp() as updated_timestamp,
        current_timestamp() as inserted_timestamp,
        -- Track which streams contributed to this record
        array_construct_compact(
            case when base.consumerid is not null then 'stream_consumer' end,
            case when ext.consumerid is not null then 'stream_consumer_extension' end,
            case when crm.consumerid is not null then 'stream_crm_consumer' end,
            case when real_person.consumerid is not null then 'stream_consumer_isrealperson' end,
            case when seasonal.consumerid is not null then 'stream_consumer_seasonal_address_selector' end,
            case when email_mkt.consumerid is not null then 'stream_consumer_email_marketing_override' end,
            case when dates.consumerid is not null then 'stream_special_dates' end,
            case when addl_attr.consumerid is not null then 'stream_additional_attributes' end,
            case when append_attr.consumerid is not null then 'stream_append_attributes' end,
            case when travel.consumerid is not null then 'stream_travel_segment' end,
            case when hotel.consumerid is not null then 'stream_hotel_travel_segment' end,
            case when profile_seg.consumerid is not null then 'stream_profile_segment' end,
            case when profile_pref.consumerid is not null then 'stream_profile' end,
            case when addr_norm.consumerid is not null then 'stream_address_normalized' end,
            case when email_val.consumerid is not null then 'stream_email_validation' end,
            case when agency.consumerid is not null then 'stream_agency_advisor_map' end,
            case when excluded.consumerid is not null then 'stream_consumer_isexcluded' end,
            case when spend.consumerid is not null then 'stream_consumer_totalspend' end,
            case when soft_del.consumerid is not null then 'stream_consumer_soft_deleted' end
        ) as source_streams
        
    from all_consumer_ids ids
    full outer join base_consumer base on ids.consumerid = base.consumerid
    full outer join consumer_extension ext on ids.consumerid = ext.consumerid
    full outer join crm_consumer crm on ids.consumerid = crm.consumerid
    full outer join consumer_isrealperson real_person on ids.consumerid = real_person.consumerid
    full outer join seasonal_address seasonal on ids.consumerid = seasonal.consumerid
    full outer join email_marketing_override email_mkt on ids.consumerid = email_mkt.consumerid
    full outer join special_dates dates on ids.consumerid = dates.consumerid
    full outer join additional_attributes addl_attr on ids.consumerid = addl_attr.consumerid
    full outer join consumer_append_attributes append_attr on ids.consumerid = append_attr.consumerid
    full outer join travel_segment travel on ids.consumerid = travel.consumerid
    full outer join hotel_travel_segment hotel on ids.consumerid = hotel.consumerid
    full outer join profile_segment profile_seg on ids.consumerid = profile_seg.consumerid
    full outer join profile_preferences profile_pref on ids.consumerid = profile_pref.consumerid
    full outer join address_normalized addr_norm on ids.consumerid = addr_norm.consumerid
    full outer join email_validation email_val on ids.consumerid = email_val.consumerid
    full outer join agency_advisor_map agency on ids.consumerid = agency.consumerid
    full outer join consumer_excluded excluded on ids.consumerid = excluded.consumerid
    full outer join consumer_totalspend spend on ids.consumerid = spend.consumerid
    full outer join consumer_soft_deleted soft_del on ids.consumerid = soft_del.consumerid
)

select * from final_consumer_master 