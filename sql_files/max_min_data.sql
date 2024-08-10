SELECT
    DATE(datetime) AS date,
    MAX(high) AS daily_high,
    MIN(low) AS daily_low
FROM
    tsla_daily_data
GROUP BY
    DATE(datetime)
ORDER BY
    date DESC;