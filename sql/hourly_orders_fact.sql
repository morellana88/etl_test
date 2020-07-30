WITH
  od AS (
  SELECT
    order_id,
    user_id,
    order_number,
    order_dow day_key,
    -- Assuming the hour 24 is the 00:00 next day
    IF(order_hour_of_day > 23, 0, order_dow + 1) hour_key,
    product_name,
    product_number
  FROM
    `order_details`) 
SELECT
  order_id,
  user_id,
  order_number,
  -- Adjusting the start day after the hour workaround
  IF(od.day_key > 6, 0, od.day_key) day_key,
  od.hour_key,
  p.product_key,
  od.product_name,
  od.product_number
FROM
  od
LEFT JOIN `product_dim` p ON p.product_name = od.product_name
