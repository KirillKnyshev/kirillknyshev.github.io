-- Task One 
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

-- Task Two
-- 1. SET CONTRACT PARAMETERS
DECLARE injection_date DATE DEFAULT '2024-06-15';
DECLARE withdrawal_date DATE DEFAULT '2024-12-15';
DECLARE quantity FLOAT64 DEFAULT 1000000; -- 1M MMBtu
DECLARE storage_cost_per_month FLOAT64 DEFAULT 100000;
DECLARE injection_withdrawal_cost_per_unit FLOAT64 DEFAULT 0.01; 
DECLARE transport_cost FLOAT64 DEFAULT 50000;

-- 2. BUILD THE COMPLETE PRICE CURVE
WITH historical AS (
  SELECT Dates, Prices FROM `case-studies-478607.jpmorgan_analysis.nat_gas`
),
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
-- 3. INTERPOLATE PRICES
bounds AS (
  SELECT 
    Dates AS start_date, Prices AS start_price,
    LEAD(Dates) OVER(ORDER BY Dates) AS end_date,
    LEAD(Prices) OVER(ORDER BY Dates) AS end_price
  FROM full_curve
),
interpolated_prices AS (
  SELECT 
    input_date,
    start_price + (end_price - start_price) * (DATE_DIFF(input_date, start_date, DAY) / NULLIF(DATE_DIFF(end_date, start_date, DAY), 0)) AS price
  FROM bounds, UNNEST([injection_date, withdrawal_date]) AS input_date
  WHERE input_date >= start_date AND input_date < end_date
),
-- 4. AGGREGATE RESULTS
contract_values AS (
  SELECT 
    MAX(CASE WHEN input_date = injection_date THEN price END) AS buy_price,
    MAX(CASE WHEN input_date = withdrawal_date THEN price END) AS sell_price
  FROM interpolated_prices
)
-- 5. FINAL CALCULATION
SELECT 
    ROUND(buy_price, 4) AS injection_price,
    ROUND(sell_price, 4) AS withdrawal_price,
    ROUND((sell_price - buy_price) * quantity, 2) AS gross_profit,
    ROUND(DATE_DIFF(withdrawal_date, injection_date, MONTH) * storage_cost_per_month, 2) AS total_storage_costs,
    ROUND((quantity * injection_withdrawal_cost_per_unit * 2) + (transport_cost * 2), 2) AS total_ops_costs,
    ROUND(((sell_price - buy_price) * quantity) 
    - (DATE_DIFF(withdrawal_date, injection_date, MONTH) * storage_cost_per_month)
    - (quantity * injection_withdrawal_cost_per_unit * 2)
    - (transport_cost * 2), 2) AS net_contract_value
FROM contract_values;

-- Task Three
SELECT 
    customer_id,
    fico_score,
    loan_amt_outstanding, -- Fixed column name
    total_debt_outstanding,
    income,
    -- 1. Categorizing Risk Tiers
    CASE 
        WHEN fico_score >= 750 THEN 'Low Risk'
        WHEN fico_score >= 670 THEN 'Medium Risk'
        WHEN fico_score >= 580 THEN 'High Risk'
        ELSE 'Critical Risk'
    END AS risk_category,
    -- 2. Estimating Probability of Default (PD)
    CASE 
        WHEN fico_score >= 750 THEN 0.02 
        WHEN fico_score >= 670 THEN 0.08 
        WHEN fico_score >= 580 THEN 0.20 
        ELSE 0.45 
    END AS estimated_pd,
    -- 3. Calculating Expected Loss (assuming 10% recovery as per task)
    -- Formula: (Loan Amount * PD) * (1 - Recovery Rate)
    ROUND((loan_amt_outstanding * (
        CASE 
            WHEN fico_score >= 750 THEN 0.02 
            WHEN fico_score >= 670 THEN 0.08 
            WHEN fico_score >= 580 THEN 0.20 
            ELSE 0.45 
        END
    )) * 0.9, 2) AS expected_loss
FROM `case-studies-478607.jpmorgan_analysis.loan_data`;

-- Task Four
