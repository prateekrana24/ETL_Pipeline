/*
The code is used to analyze the stock's performance by calculating short-term (10-day) 
and long-term (50-day) moving averages. Traders often use these averages to identify 
potential buy or sell signals based on trends. 
For example, if the short-term moving average crosses above the long-term moving average, 
it might indicate an uptrend, and vice versa.

The Volume-Weighted Average Price (VWAP) is a trading benchmark used to give the average 
price a security has traded at throughout the day, based on both volume and price. 
Unlike a simple moving average, which only considers prices, VWAP accounts for the 
number of shares traded at different price levels.

VWAP is often used by traders to determine the market's general direction or to execute 
orders in a way that ensures their trades align with the overall market trend.
*/

DROP TABLE IF EXISTS volume_weighted_avg_price;

CREATE TABLE volume_weighted_avg_price AS

SELECT
    DATE(datetime) AS date,
    SUM(volume * close) / SUM(volume) AS vwap
FROM
    tsla_daily_data
GROUP BY
    DATE(datetime)
ORDER BY
    date DESC;
