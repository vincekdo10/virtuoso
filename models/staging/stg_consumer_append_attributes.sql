{{ config(
    materialized='incremental',
    unique_key='consumerid',
    incremental_strategy='merge',
    on_schema_change='sync_all_columns',
    snowflake_warehouse='WH_PIPE_COMMISSION'
) }}

with source_data as (
    select aa.*
    from {{ source('consumer', 'stream_append_attributes') }} aa
    join {{ source('consumer', 'consumer') }} c 
        on aa.consumerid = c.consumerid
    where aa.metadata$action = 'INSERT'
    
    {% if is_incremental() %}
        and aa.consumerid in (
            select distinct consumerid 
            from {{ source('consumer', 'stream_append_attributes') }}
            where metadata$action = 'INSERT'
        )
    {% endif %}
),

transformed as (
    select
        consumerid,
        -- Age and demographic fields
        age,
        agerange_18_24yearsold,
        agerange_25_34yearsold,
        agerange_35_44yearsold,
        agerange_45_54yearsold,
        agerange_55_64yearsold,
        agerange_65_74yearsold,
        agerange_75plusyearsold,
        age_year_of_birth,
        age_month_of_birth,
        
        -- Financial and purchasing behavior
        boughtusingamex,
        buyer_high_end_spirit_drinkers,
        buyer_imported_beer_enthusiast,
        buyerhighscalecatalogorders,
        buyerorderstravel,
        censusincomepercentile,
        collectibles,
        
        -- Geographic and location data
        county_fips,
        county_name,
        dma,
        dma_code,
        msa,
        
        -- Children and family data
        children_age_0_3_gender,
        children_age_4_6_gender,
        children_age_7_9_gender,
        children_age_10_12_gender,
        children_age_13_15_gender,
        children_age_16_18_gender,
        
        -- Consumer buying styles
        consumer_buying_style_email,
        consumer_buying_style_savvy_researchers,
        consumer_buying_style_organic_and_natural,
        consumer_buying_style_brand_loyalists,
        consumer_buying_style_trendsetters,
        consumer_buying_style_deal_seekers,
        consumer_buying_style_recreational_shoppers,
        consumer_buying_style_in_the_moment_shoppers,
        consumer_buying_style_quality_matters,
        consumer_buying_style_mainstream_adopters,
        consumer_buying_style_novelty_seekers,
        
        -- Advertising channel preferences
        consumer_preferred_adv_channel_broadcast_cable_tv,
        consumer_preferred_adv_channel_digital_display,
        consumer_preferred_adv_channel_direct_mail,
        consumer_preferred_adv_channel_digital_newspaper,
        consumer_preferred_adv_channel_digital_video,
        consumer_preferred_adv_channel_radio,
        consumer_preferred_adv_channel_streaming_tv,
        consumer_preferred_adv_channel_traditional_newspaper,
        consumer_preferred_adv_channel_mobile_sms_mms,
        
        -- Housing and dwelling data
        donor,
        dwellingtype_multifamily,
        dwellingtype_pobox,
        dwellingtype_singlefamily,
        dwelling_number_of_units,
        
        -- Education levels
        education_lessthanhighschooldiploma,
        education_highschooldiploma,
        education_somecollege,
        education_bachelordegree,
        education_graduatedegree,
        education_unknown,
        educationlevel,
        
        -- Family and lifestyle
        family_new_parent_36m_indicator,
        gender,
        geovectordesc,
        grandchildren,
        hastakencruisevacation,
        
        -- Hobbies and interests
        hobby_birdwatching,
        hobby_books_literature,
        interest_book_reader,
        interest_e_book_reader,
        interest_audio_book_listener,
        interest_pet_enthusiastinterest_pet_enthusiast,
        hobby_casinogambling,
        hobby_cigarsmoking,
        hobby_cooking,
        hobby_crafts,
        hobby_cultural_history,
        hobby_diy,
        hobby_entrepreneur,
        hobby_exercise3ormoretimesweek,
        hobby_gardening,
        hobby_gourmetcooking,
        hobby_homeliving,
        hobby_mens_fashion,
        hobby_news_politics,
        hobby_photography,
        hobby_selfimprovementcourses,
        hobby_theaterarts,
        hobby_wineappreciation,
        hobby_womens_fashion,
        hobby_computers_electronics,
        
        -- Home ownership and value
        homeownership_owner,
        homeownership_renter,
        homevaluerange,
        
        -- Household composition
        householdcompositionrollup_adultfemalewchildren,
        householdcompositionrollup_adultfemale,
        householdcompositionrollup_adultmalefemalewchildren,
        householdcompositionrollup_adultmalefemale,
        householdcompositionrollup_adultmalewchildren,
        householdcompositionrollup_adultmale,
        
        -- Income and financial data
        incomerange,
        investments,
        lengthofresidence,
        
        -- Lifestyle and behavior
        lifestyle_proud_grandparent_survey,
        lifestyle_buys_via_direct_mail_survey,
        mailorderresponder,
        
        -- Marital status
        maritalstatus_married,
        maritalstatus_single,
        
        -- Moving and residence
        movedbetween_dec_12_2022_and_nov_30_2023,
        
        -- Household demographics
        networth,
        numberofadultsinhh,
        numberofchildren,
        numberofchildreninhh,
        numberofpersonsinhh,
        
        -- Occupations
        occupation_agricultureenvironment,
        occupation_bluecollar,
        occupation_business_owner,
        occupation_businessadmin,
        occupation_clergy,
        occupation_disabled,
        occupation_education,
        occupation_finance,
        occupation_legal,
        occupation_sales,
        occupation_medical,
        occupation_militarygovernment,
        occupation_operatehomebusiness,
        occupation_retired,
        occupation_selfemployed,
        occupation_student,
        occupation_whitecollar,
        
        -- Pet ownership
        ownscat,
        ownsdog,
        
        -- Geographic and demographic percentiles
        percentsinglefamilydwelling,
        premiumamex,
        
        -- Presence of children by age groups
        presenceofchildren0_2,
        presenceofchildren11_15,
        presenceofchildren16_17,
        presenceofchildren3_5,
        presenceofchildren6_10,
        presenceofelderlyparent,
        presence_of_young_adult,
        presence_of_child_overall_0_18,
        
        -- Religion and lifestyle
        religion,
        
        -- Sports and activities
        sports_biking,
        sports_boating,
        sports_campinghiking,
        sports_fishing,
        sports_fitness,
        sports_general,
        sports_golf,
        sports_nascar,
        sports_scuba,
        sports_skiing,
        sports_tennis,
        sports_walking,
        
        -- Travel and entertainment interests
        interest_disney_park_fan,
        interest_july_4th_travelers_lm,
        interest_summer_break_travelers_lm,
        interest_theme_parks_lm,
        interest_travel_rewards,
        interest_frequent_air_traveler,
        interest_sweepstakes_lottery,
        interest_weight_conscious,
        interest_on_a_diet,
        interest_healthy_living,
        interest_sports_enthusiast,
        interest_snow_sports,
        interests_travel_bb,
        interests_affluent_lifestyle_bb,
        interest_travel_cruise,
        interest_travel_time_share,
        interest_travel_business_travel,
        interest_travel_personal_travel,
        interest_travel_domestic,
        
        -- Travel expenditure rankings
        travel_expenditures_rank_0_25,
        travel_expenditures_rank_26_50,
        travel_expenditures_rank_51_75,
        travel_expenditures_rank_76_90,
        travel_expenditures_rank_91_100,
        travel_expenditures_annual_spend,
        
        -- Additional interests
        interest_amusement_parks,
        interest_zoos,
        interest_coffee_connoisseurs,
        interest_wine_lovers,
        interest_home_improvement_spenders,
        interest_hunting_enthusiasts,
        interest_digital_magazine_newspapers_buyers,
        interest_attend_or_ordereducationalprograms,
        interest_eats_at_family_restaurants,
        interest_eats_at_fast_food_restaurants,
        interest_canoeing_kayaking,
        interest_gourmet_cooking,
        interest_avid_runners,
        interest_outdoor_enthusiast,
        
        -- Lifestyle and travel behavior
        lifestyle_high_frequency_business_traveler,
        lifestyle_high_frequency_cruise_enthusiast,
        lifestyle_high_frequency_domestic_vacationer,
        lifestyle_high_frequency_foreign_vacationer,
        lifestyle_frequent_flyer_program_member,
        lifestyle_hotel_guest_loyalty_program,
        
        -- Travel preferences
        travel_casinovacations,
        travel_familyvacations,
        travel_frequentflyers,
        travel_international,
        travel_personalservices,
        travel_us,
        
        -- Shopping and lifestyle
        upscaleretail,
        
        -- Urban/Metro classifications
        "urban_metro_urban20k+metroadjacent",
        urban_metro_metrocountieslessthan250k,
        "urban_metro_ruralOrlessthan2.5kurban_not_adjacent",
        "urban_metro_ruralOrlessthan2.5kurban_adjacent",
        "urban_metro_metrocounties250k-1m",
        "urban_metro_urban_2.5k-19.99k_not_adjacent",
        "urban_metro_urban2.5k-19.99kadjacent",
        "urban_metro_urban_20k+notadjacent",
        "urban_metro_metro counties 1m+",
        
        -- Wealth and demographic data
        wealthpercentile,
        mosaic,
        mosaic_group,
        
        -- Timestamps
        current_timestamp() as updated_timestamp,
        current_timestamp() as inserted_timestamp,
        'stream_append_attributes' as source_stream
        
    from source_data
)

select * from transformed 