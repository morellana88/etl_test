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