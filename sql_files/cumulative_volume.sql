/*
Cumulative volume in stocks refers to the total trading volume accumulated over a 
specific period. It tracks how the total number of shares traded adds up over time. 
Investors and analysts use cumulative volume to understand trading activity trends and 
liquidity, and to identify patterns in trading behavior.

For instance, if you are tracking Tesla stock, the cumulative volume for a specific date 
would be the sum of all trading volumes from the start of your data set up to and including 
that date. It helps in visualizing the total trading activity and can indicate periods of 
increased or decreased trading interest.
Tracking cumulative volume helps in:

Identifying Trends: Understanding whether trading activity is increasing or 
decreasing over time.

Analyzing Liquidity: Higher cumulative volume can indicate greater 
liquidity and interest in the stock.

Spotting Anomalies: Significant changes in cumulative volume might indicate unusual 
trading activity or significant news affecting the stock.
*/

DROP TABLE IF EXISTS cumulative_volume;

CREATE TABLE cumulative_volume AS

SELECT
    datetime,
    volume,
    SUM(volume) OVER (ORDER BY datetime) AS cumulative_volume
FROM
    tsla_daily_data;