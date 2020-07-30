SELECT
    FARM_FINGERPRINT(product_name) product_key,
    product_name,
    aisle,
    department,
    added_by,
    added_timestamp
FROM
    `products`