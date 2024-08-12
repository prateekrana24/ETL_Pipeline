/*
A price breakout in the context of stocks occurs when the price of a stock moves above a resistance level or 
below a support level, indicating a significant change in the market's sentiment. Typically, a breakout is 
seen as a signal of a potential strong movement in the direction of the breakout.

For example:

Resistance Level: A price point where the stock has had difficulty rising above in the past. 
When the stock's price moves above this level, it is considered a bullish breakout.

Support Level: A price point where the stock has had difficulty falling below. When the stock's price falls below this level, 
it is considered a bearish breakout.

Breakouts are often followed by increased trading volume, as traders and investors see this as a 
signal to buy or sell the stock, depending on the direction of the breakout.
*/

DROP TABLE IF EXISTS price_breakouts;

CREATE TABLE price_breakouts AS

WITH rolling_highs AS (
    SELECT
        datetime,
        high,
        MAX(high) OVER (ORDER BY datetime ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) AS high_10_day
    FROM
        tsla_daily_data
)
SELECT
    datetime,
    high,
    high_10_day,
    CASE
        WHEN high > high_10_day THEN 'Breakout'
        ELSE 'No Breakout'
    END AS breakout_status
FROM
    rolling_highs
WHERE
    TIME(datetime) = '16:00:00';