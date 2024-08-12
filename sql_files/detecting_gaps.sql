/*
Purpose: The query is designed to detect gaps in stock prices. 
A gap occurs when there is a significant difference between the stock's closing price 
on one day and its opening price on the next trading day. These gaps can be important 
for traders and analysts for several reasons:

Market Reactions: Gaps can indicate significant market reactions to news 
or events that occurred after the market closed.

Trend Reversals: Large gaps might signal a potential reversal in the stock's trend 
or confirm the continuation of a trend.

Trading Strategies: Some trading strategies focus on gaps, 
using them to identify entry or exit points.
*/

DROP TABLE IF EXISTS detecting_gaps;

CREATE TABLE detecting_gaps AS

SELECT
    datetime,
    open,
    LAG(close) OVER (ORDER BY datetime) AS previous_close,
    open - LAG(close) OVER (ORDER BY datetime) AS price_gap
FROM
    tsla_daily_data
WHERE
    TIME(datetime) = '09:30:00'  -- Assuming the market opens at 9:30 AM
ORDER BY
    datetime;
