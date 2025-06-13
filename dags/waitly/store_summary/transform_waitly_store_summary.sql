-- Insert only unprocessed data
    INSERT INTO TDP_DEV.BRONZE.WAITLY_STORE_SUMMARY
    SELECT
        json_content:timestamp::STRING AS timestamp,  
        l.value:id::STRING AS id,
        l.value:externalId::STRING AS external_id,
        l.value:name::STRING AS name,
        l.value:waiting::NUMBER AS waiting,
        l.value:noShows::NUMBER AS no_shows,
        l.value:partiesServed::NUMBER AS parties_served,
        l.value:guestsServed::NUMBER AS guests_served,
        l.value:reservationsServed::NUMBER AS reservations_served,
        l.value:longestWait::NUMBER AS longest_wait,
        l.value:waitingOnline::NUMBER AS waiting_online,
        l.value:waitTime15MinuteMovingAverage.value::FLOAT AS wait_time_value,
        l.value:waitTime15MinuteMovingAverage.sampleSize::NUMBER AS wait_time_sample_size,
        l.value:waitTime15MinuteMovingAverage.minutesWaited::NUMBER AS wait_time_minutes_waited,

        -- Extract category values into separate columns
        MAX(CASE WHEN c.value:name::STRING = 'Pickup' THEN c.value:value::FLOAT END) AS movingAverage15ByCategoryPickup,
        MAX(CASE WHEN c.value:name::STRING = 'Pickup' THEN c.value:sampleSize::NUMBER END) AS movingAverage15ByCategoryPickupSampleSize,

        MAX(CASE WHEN c.value:name::STRING = 'Walk-in' THEN c.value:value::FLOAT END) AS movingAverage15ByCategoryWalk_in,
        MAX(CASE WHEN c.value:name::STRING = 'Walk-in' THEN c.value:sampleSize::NUMBER END) AS movingAverage15ByCategoryWalk_inSampleSize,

        MAX(CASE WHEN c.value:name::STRING = 'Express' THEN c.value:value::FLOAT END) AS movingAverage15ByCategoryExpress,
        MAX(CASE WHEN c.value:name::STRING = 'Express' THEN c.value:sampleSize::NUMBER END) AS movingAverage15ByCategoryExpressSampleSize,

        MAX(CASE WHEN c.value:name::STRING = 'Delivery' THEN c.value:value::FLOAT END) AS movingAverage15ByCategoryDelivery,
        MAX(CASE WHEN c.value:name::STRING = 'Delivery' THEN c.value:sampleSize::NUMBER END) AS movingAverage15ByCategoryDeliverySampleSize

    FROM TDP_DEV.BRONZE.WAITLY_STORE_SUMMARY_RAW
    JOIN LATERAL FLATTEN(input => json_content:locations) l
    JOIN LATERAL FLATTEN(input => l.value:waitTime15MinuteMovingAverage.byCategory) c
    WHERE processed = FALSE -- Process only unprocessed files
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14;

    -- Mark processed records
    UPDATE TDP_DEV.BRONZE.WAITLY_STORE_SUMMARY_RAW
    SET processed = TRUE
    WHERE processed = FALSE;
