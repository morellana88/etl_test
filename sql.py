hour_dim = """
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
"""

day_dim = """
SELECT
  CAST(FORMAT_DATE('%w', d) AS INT64) AS day_key,
  d AS date,
  FORMAT_DATE('%A', d) AS day_name,
  (CASE WHEN FORMAT_DATE('%A', d) IN ('Sunday', 'Saturday') THEN 0 ELSE 1 END) AS is_weekend,
FROM (
  SELECT
    *
  FROM
    -- Random week to generate the seven day names
    UNNEST(GENERATE_DATE_ARRAY('2020-07-19', '2020-07-25', INTERVAL 1 DAY)) AS d )
"""