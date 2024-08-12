/*
A moving average in stocks is a commonly used technical analysis indicator that helps smooth out 
price data by creating a constantly updated average price. The moving average takes the average 
of a specific number of data points over a set period of time and moves forward with each new data point. 
Traders use moving averages to identify trends by filtering out the "noise" from random short-term price 
fluctuations.
*/

DROP TABLE IF EXISTS moving_averages;

CREATE TABLE moving_averages AS

SELECT
    DATE(datetime) AS date,
    close AS close_price,
    AVG(close) OVER (ORDER BY datetime ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) AS moving_avg_10,
    AVG(close) OVER (ORDER BY datetime ROWS BETWEEN 49 PRECEDING AND CURRENT ROW) AS moving_avg_50
FROM
    tsla_daily_data
WHERE
    TIME(datetime) = '16:00:00';  -- Close price of each day
