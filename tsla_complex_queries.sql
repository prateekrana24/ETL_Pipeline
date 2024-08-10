CREATE TABLE daily_returns AS
SELECT
    DATE(datetime) AS date,
    close AS close_price,
    LAG(close) OVER (ORDER BY datetime) AS previous_close,
    (close - LAG(close) OVER (ORDER BY datetime)) / LAG(close) OVER (ORDER BY datetime) * 100 AS daily_return
FROM
    tsla_daily_data
WHERE
    TIME(datetime) = '16:00:00';  -- Assuming market closes at 4 PM
    
-- ---------------------------------------------------------------------
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
