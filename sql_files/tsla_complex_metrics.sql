/*
This SQL code is a complex query that calculates various financial metrics and trends based on Tesla's stock data. 
It uses several Common Table Expressions (CTEs) to break down the query into manageable parts. Let's go through each 
section in detail.
*/
DROP TABLE IF EXISTS tsla_complex_metrics;

CREATE TABLE tsla_complex_metrics AS
-- EXPLAIN
WITH 

-- Calculate daily returns and price gaps
price_analysis AS (
    SELECT
        DATE(datetime) AS date,
        open,
        close,
        high,
        low,
        volume,
        LAG(close) OVER (ORDER BY datetime) AS previous_close,
        (close - LAG(close) OVER (ORDER BY datetime)) / LAG(close) OVER (ORDER BY datetime) * 100 AS daily_return,
        open - LAG(close) OVER (ORDER BY datetime) AS price_gap
    FROM
        tsla_daily_data
    WHERE
        TIME(datetime) = '16:00:00'  -- Market close time
),

-- Calculate 10-day and 50-day moving averages
moving_averages AS (
    SELECT
        date,
        close,
        AVG(close) OVER (ORDER BY date ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) AS ma_10_day,
        AVG(close) OVER (ORDER BY date ROWS BETWEEN 49 PRECEDING AND CURRENT ROW) AS ma_50_day
    FROM
        price_analysis
),

-- Calculate rolling volatility (standard deviation of daily returns over 10 days)
volatility AS (
    SELECT
        date,
        close,
        STDDEV_SAMP(daily_return) OVER (ORDER BY date ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) AS rolling_volatility
    FROM
        price_analysis
),

-- Calculate cumulative volume and average volume over the past 10 days
volume_trends AS (
    SELECT
        date,
        volume,
        SUM(volume) OVER (ORDER BY date) AS cumulative_volume,
        AVG(volume) OVER (ORDER BY date ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) AS avg_volume_10_day
    FROM
        price_analysis
)

-- Final selection combining all the metrics
SELECT
    p.date,
    p.open,
    p.close,
    p.high,
    p.low,
    p.volume,
    p.daily_return,
    p.price_gap,
    ma.ma_10_day,
    ma.ma_50_day,
    v.rolling_volatility,
    vt.cumulative_volume,
    vt.avg_volume_10_day,
    CASE
        WHEN p.close > ma.ma_10_day AND ma.ma_10_day > ma.ma_50_day THEN 'Uptrend'
        WHEN p.close < ma.ma_10_day AND ma.ma_10_day < ma.ma_50_day THEN 'Downtrend'
        ELSE 'Sideways'
    END AS trend_direction,
    CASE
        WHEN p.price_gap > 0 THEN 'Gap Up'
        WHEN p.price_gap < 0 THEN 'Gap Down'
        ELSE 'No Gap'
    END AS gap_type,
    CASE
        WHEN vt.avg_volume_10_day > (SELECT AVG(volume) FROM price_analysis) THEN 'High Volume'
        ELSE 'Low Volume'
    END AS volume_condition
FROM
    price_analysis p
    JOIN moving_averages ma ON p.date = ma.date
    JOIN volatility v ON p.date = v.date
    JOIN volume_trends vt ON p.date = vt.date
ORDER BY
    p.date;
    
