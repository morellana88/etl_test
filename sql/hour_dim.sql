SELECT
    CAST(EXTRACT(HOUR FROM TIME(d)) AS INT64) hour,
    FORMAT_TIME("%R", TIME(d)) as hour_minute,
    FORMAT_TIME("%I", TIME(d)) as zeropad_hour,
    FORMAT_TIME("%I:%M %p", TIME(d)) as twelve_hour_format
    FROM (
        SELECT
            *
        FROM
            -- Random day to generate the 23 hours
            UNNEST(GENERATE_TIMESTAMP_ARRAY('2020-07-26 00:00:00', 
            '2020-07-26 23:00:00', INTERVAL 1 HOUR)) AS d
    )