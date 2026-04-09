-- Task One 
-- 1. Create a Master Table of Historical + Forecasted Data
WITH historical AS (
  SELECT Dates, Prices, 'Historical' AS type FROM `case-studies-478607.jpmorgan_analysis.nat_gas`
),
-- 2. Manual Trend Calculation (Least Squares Method)
trend_calc AS (
  SELECT 
    (COUNT(*) * SUM(UNIX_DATE(Dates) * Prices) - SUM(UNIX_DATE(Dates)) * SUM(Prices)) / 
    (COUNT(*) * SUM(POW(UNIX_DATE(Dates), 2)) - POW(SUM(UNIX_DATE(Dates)), 2)) AS slope,
    AVG(Prices) - ((COUNT(*) * SUM(UNIX_DATE(Dates) * Prices) - SUM(UNIX_DATE(Dates)) * SUM(Prices)) / 
    (COUNT(*) * SUM(POW(UNIX_DATE(Dates), 2)) - POW(SUM(UNIX_DATE(Dates)), 2))) * AVG(UNIX_DATE(Dates)) AS intercept,
    AVG(Prices) as overall_avg
  FROM historical
),
-- 3. Monthly Seasonality
seasonal_averages AS (
  SELECT EXTRACT(MONTH FROM Dates) AS m_num, AVG(Prices) AS m_avg FROM historical GROUP BY 1
),
-- 4. Generate 12 months ahead (No holiday logic needed)
forecasted AS (
  SELECT 
    future_date AS Dates,
    ROUND((slope * UNIX_DATE(future_date) + intercept) + (m_avg - (SELECT overall_avg FROM trend_calc)), 4) AS Prices,
    'Forecast' AS type
  FROM UNNEST(GENERATE_DATE_ARRAY('2024-10-31', '2025-09-30', INTERVAL 1 MONTH)) AS future_date
  CROSS JOIN trend_calc
  JOIN seasonal_averages ON EXTRACT(MONTH FROM future_date) = m_num
),
-- 5. Combine into one Time Series
full_series AS (
  SELECT * FROM historical UNION ALL SELECT * FROM forecasted
)
SELECT 
    Dates, 
    Prices, 
    type,
    -- This handles the 'Price at any date' request by showing the slope to the next point
    LEAD(Prices) OVER(ORDER BY Dates) AS next_price,
    DATE_DIFF(LEAD(Dates) OVER(ORDER BY Dates), Dates, DAY) AS days_to_next
FROM full_series
ORDER BY Dates;




-- SET YOUR DATE HERE
DECLARE input_date DATE DEFAULT '2025-09-15'; 

WITH historical AS (
  SELECT Dates, Prices FROM `case-studies-478607.jpmorgan_analysis.nat_gas`
),
-- 1. Calculate the Trend & Seasonality for the Forecast
trend_calc AS (
  SELECT 
    (COUNT(*) * SUM(UNIX_DATE(Dates) * Prices) - SUM(UNIX_DATE(Dates)) * SUM(Prices)) / 
    (COUNT(*) * SUM(POW(UNIX_DATE(Dates), 2)) - POW(SUM(UNIX_DATE(Dates)), 2)) AS slope,
    AVG(Prices) - ((COUNT(*) * SUM(UNIX_DATE(Dates) * Prices) - SUM(UNIX_DATE(Dates)) * SUM(Prices)) / 
    (COUNT(*) * SUM(POW(UNIX_DATE(Dates), 2)) - POW(SUM(UNIX_DATE(Dates)), 2))) * AVG(UNIX_DATE(Dates)) AS intercept,
    AVG(Prices) as overall_avg
  FROM historical
),
seasonal_averages AS (
  SELECT EXTRACT(MONTH FROM Dates) AS m_num, AVG(Prices) AS m_avg FROM historical GROUP BY 1
),
-- 2. Combine Historical and Forecasted into one Master List
full_curve AS (
  SELECT Dates, Prices FROM historical
  UNION ALL
  SELECT 
    f_date,
    (slope * UNIX_DATE(f_date) + intercept) + (m_avg - (SELECT overall_avg FROM trend_calc))
  FROM UNNEST(GENERATE_DATE_ARRAY('2024-10-31', '2025-09-30', INTERVAL 1 MONTH)) AS f_date
  CROSS JOIN trend_calc
  JOIN seasonal_averages ON EXTRACT(MONTH FROM f_date) = m_num
),
-- 3. Find the two closest dates to our 'input_date'
bounds AS (
  SELECT 
    Dates AS start_date,
    Prices AS start_price,
    LEAD(Dates) OVER(ORDER BY Dates) AS end_date,
    LEAD(Prices) OVER(ORDER BY Dates) AS end_price
  FROM full_curve
)
-- 4. Final Interpolation Calculation
SELECT 
  input_date,
  ROUND(start_price + (end_price - start_price) * (DATE_DIFF(input_date, start_date, DAY) / NULLIF(DATE_DIFF(end_date, start_date, DAY), 0)), 4) AS estimated_price
FROM bounds
WHERE input_date >= start_date AND input_date < end_date;
