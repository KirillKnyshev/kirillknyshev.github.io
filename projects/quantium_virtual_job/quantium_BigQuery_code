-- looking for nulls 
SELECT 
  countif(DATE IS NULL) AS null_dates,
  countif(STORE_NBR IS NULL) AS null_stores,
  countif(LYLTY_CARD_NBR IS NULL) AS null_customers,
  countif(PROD_NAME IS NULL) AS null_products,
  countif(TOT_SALES IS NULL) AS null_sales
FROM `case-studies-478607.quantium_analysis.qvi_transaction_data`;

-- Identifying commercial outliers (Customers buying 200+ packs)
SELECT * 
FROM `case-studies-478607.quantium_analysis.qvi_transaction_data`;
WHERE PROD_QTY >= 200;

-- Removing the outlier (Loyalty Card 226000) for clean analysis, and creating a new clean transaction table 
CREATE OR REPLACE TABLE `case-studies-478607.quantium_analysis.clean_transactions_data` AS
SELECT * 
FROM `case-studies-478607.quantium_analysis.qvi_transaction_data`;
WHERE LYLTY_CARD_NBR != 226000;

-- "TRIM" to remove start/end spaces, "REGEXP_REPLACE" handles middle double-spaces.
CREATE OR REPLACE TABLE `case-studies-478607.quantium_analysis.clean_transactions_data` AS
 SELECT *,TRIM(REGEXP_REPLACE(PROD_NAME, r'\s+', ' ')) AS NEW_PROD_NAME
 FROM `case-studies-478607.quantium_analysis.clean_transactions_data`;

-- fix common shorthand and missing spaces. (one of many fixes that need to be made throughout 'Prod_Name'), using the "REPLACE" method
CREATE OR REPLACE TABLE `case-studies-478607.quantium_analysis.clean_transactions_data` AS
SELECT * EXCEPT(PROD_NAME),
  REPLACE(
    REPLACE(
      REPLACE(
        REPLACE(PROD_NAME, '&Chives', '& Chives'), 
      'Sr/Cream', 'Sour Cream'),
    'Chp', 'Chip'),
  'Swt', 'Sweet') AS PROD_NAME
FROM `case-studies-478607.quantium_analysis.clean_transactions_data`;

--MAX and MIN
SELECT MAX(TOT_SALES), MIN(TOT_SALES)
FROM `case-studies-478607.quantium_analysis.clean_transactions_data`

-- Joining Transaction data with Customer Demographics
SELECT 
  t.*, 
  c.LIFESTAGE, 
  c.PREMIUM_CUSTOMER
FROM `quantium-strategy.retail_data.transactions` AS t
LEFT JOIN `quantium-strategy.retail_data.customer_demographics` AS c
ON t.LYLTY_CARD_NBR = c.LYLTY_CARD_NBR;

-- Separating brand from Prod_Name column
CREATE OR REPLACE TABLE `case-studies-478607.quantium_analysis.clean_transactions_data` AS
SELECT 
  *,
  CASE 
    WHEN raw_brand = 'Red' THEN 'Red Rock Deli'
    WHEN raw_brand = 'Grain' THEN 'Grain Waves'
    WHEN raw_brand = 'Natural' THEN 'Natural Chip Co'
    WHEN raw_brand = 'Cobs' THEN 'Cobs Popd'
    WHEN raw_brand = 'Burger' THEN 'Smiths' -- Burger Rings are a Smiths product
    ELSE raw_brand 
  END AS BRAND
FROM (
  SELECT 
    *, 
    REGEXP_EXTRACT(PROD_NAME, r'^([^\s]+)') AS raw_brand 
  FROM `case-studies-478607.quantium_analysis.clean_transactions_data`

-- Separating pack size from Prod_Name column
CREATE OR REPLACE TABLE `case-studies-478607.quantium_analysis.clean_transactions_data` AS
SELECT *, CAST(REGEXP_EXTRACT(PROD_NAME, r'(\d+)') AS INT64) AS PACK_SIZE
FROM `case-studies-478607.quantium_analysis.clean_transactions_data`;

-- Answering Task one with SQL code in BigQuery

  -- 1. TOTAL SALES BY SEGMENT (Who spends the most?)
  SELECT 
    LIFESTAGE,
    PREMIUM_CUSTOMER,
    ROUND(SUM(TOT_SALES), 2) AS TOTAL_SALES
  FROM `case-studies-478607.quantium_analysis.transaction_purchase_behavior_merged_data`
  GROUP BY 1, 2
  ORDER BY TOTAL_SALES DESC;

  -- 2. CUSTOMER PROFILING (How many customers and how much do they buy?)
  SELECT 
    LIFESTAGE,
    PREMIUM_CUSTOMER,
    COUNT(DISTINCT LYLTY_CARD_NBR) AS UNIQUE_CUSTOMERS,
    ROUND(SUM(PROD_QTY) / COUNT(DISTINCT LYLTY_CARD_NBR), 2) AS AVG_UNITS_PER_CUST,
    ROUND(SUM(TOT_SALES) / SUM(PROD_QTY), 2) AS AVG_PRICE_PER_UNIT
  FROM `case-studies-478607.quantium_analysis.transaction_purchase_behavior_merged_data`
  GROUP BY 1, 2
  ORDER BY UNIQUE_CUSTOMERS DESC;

  -- 3. PACK SIZE ANALYSIS (What sizes do they prefer?)
  SELECT 
    LIFESTAGE,
    PREMIUM_CUSTOMER,
    ROUND(AVG(PACK_SIZE), 2) AS AVG_PACK_SIZE,
    APPROX_TOP_SUM(CAST(PACK_SIZE AS STRING), 1, 1)[OFFSET(0)].value AS MOST_COMMON_PACK
  FROM `case-studies-478607.quantium_analysis.transaction_purchase_behavior_merged_data`
  GROUP BY 1, 2
  ORDER BY AVG_PACK_SIZE DESC;

-- Answering Task Two with SQL code in BigQuery
  CREATE OR REPLACE TABLE `case-studies-478607.quantium_analysis.monthly_store_metrics` AS
  SELECT 
    STORE_NBR,
    FORMAT_DATE('%Y%m', DATE) AS month_id,
    ROUND(SUM(TOT_SALES), 2) AS total_sales,
    COUNT(DISTINCT LYLTY_CARD_NBR) AS total_customers,
    ROUND(SUM(PROD_QTY) / COUNT(DISTINCT LYLTY_CARD_NBR), 2) AS avg_chips_per_cust,
    ROUND(SUM(TOT_SALES) / NULLIF(SUM(PROD_QTY), 0), 2) AS avg_price_per_unit
  FROM `case-studies-478607.quantium_analysis.clean_transactions_data`
  GROUP BY 1, 2;

WITH trial_store_77 AS (
  SELECT month_id, total_sales, total_customers
  FROM `case-studies-478607.quantium_analysis.monthly_store_metrics`
  WHERE STORE_NBR = 77 AND month_id < '201902'
),
all_other_stores AS (
  SELECT STORE_NBR, month_id, total_sales, total_customers
  FROM `case-studies-478607.quantium_analysis.monthly_store_metrics`
  WHERE STORE_NBR != 77 AND month_id < '201902'
)
SELECT 
  others.STORE_NBR,
  -- 1. Correlation of Sales (Do the trends match?)
  ROUND(CORR(others.total_sales, trial.total_sales), 4) AS corr_sales,
  -- 2. Correlation of Customers (Do the foot-traffic patterns match?)
  ROUND(CORR(others.total_customers, trial.total_customers), 4) AS corr_customers
FROM all_other_stores AS others
JOIN trial_store_77 AS trial ON others.month_id = trial.month_id
GROUP BY 1
HAVING corr_sales IS NOT NULL
ORDER BY corr_sales DESC
LIMIT 5;

WITH metrics AS (
  SELECT 
    STORE_NBR,
    AVG(total_sales) AS avg_sales,
    AVG(total_customers) AS avg_cust
  FROM `case-studies-478607.quantium_analysis.monthly_store_metrics`
  WHERE month_id < '201902'
  GROUP BY 1
),
abs_diff AS (
  SELECT 
    STORE_NBR,
    ABS(metrics.avg_sales - (SELECT avg_sales FROM metrics WHERE STORE_NBR = 77)) AS sales_diff,
    ABS(metrics.avg_cust - (SELECT avg_cust FROM metrics WHERE STORE_NBR = 77)) AS cust_diff
  FROM metrics
)
SELECT 
  STORE_NBR,
  -- Scaling the distance: 1 is closest, 0 is furthest
  1 - (sales_diff - MIN(sales_diff) OVER()) / (MAX(sales_diff) OVER() - MIN(sales_diff) OVER()) AS mag_sales,
  1 - (cust_diff - MIN(cust_diff) OVER()) / (MAX(cust_diff) OVER() - MIN(cust_diff) OVER()) AS mag_cust
FROM abs_diff
WHERE STORE_NBR IN (233, 41, 50) -- Testing your top candidates
ORDER BY mag_sales DESC;

WITH scaling_factor AS (
  -- We calculate the ratio between the two stores BEFORE the trial
  SELECT 
    SUM(CASE WHEN STORE_NBR = 77 THEN total_sales END) / 
    SUM(CASE WHEN STORE_NBR = 233 THEN total_sales END) AS sales_ratio
  FROM `case-studies-478607.quantium_analysis.monthly_store_metrics`
  WHERE month_id < '201902'
)
SELECT 
  month_id,
  -- Actual Sales for Trial Store
  MAX(CASE WHEN STORE_NBR = 77 THEN total_sales END) AS trial_sales,
  -- Scaled Sales for Control Store (what we EXPECTED to happen)
  ROUND(MAX(CASE WHEN STORE_NBR = 233 THEN total_sales END) * (SELECT sales_ratio FROM scaling_factor), 2) AS scaled_control_sales,
  -- The Percentage Uplift
  ROUND(((MAX(CASE WHEN STORE_NBR = 77 THEN total_sales END) / (MAX(CASE WHEN STORE_NBR = 233 THEN total_sales END) * (SELECT sales_ratio FROM scaling_factor))) - 1) * 100, 2) AS percentage_uplift
FROM `case-studies-478607.quantium_analysis.monthly_store_metrics`
WHERE month_id BETWEEN '201902' AND '201904'
GROUP BY 1
ORDER BY 1;

WITH trial_store_86 AS (
  SELECT month_id, total_sales, total_customers
  FROM `case-studies-478607.quantium_analysis.monthly_store_metrics`
  WHERE STORE_NBR = 86 AND month_id < '201902'
),
all_other_stores AS (
  SELECT STORE_NBR, month_id, total_sales, total_customers
  FROM `case-studies-478607.quantium_analysis.monthly_store_metrics`
  WHERE STORE_NBR NOT IN (77, 86, 88) AND month_id < '201902'
)
SELECT 
  others.STORE_NBR,
  ROUND(CORR(others.total_sales, trial.total_sales), 4) AS corr_sales,
  ROUND(CORR(others.total_customers, trial.total_customers), 4) AS corr_customers
FROM all_other_stores AS others
JOIN trial_store_86 AS trial ON others.month_id = trial.month_id
GROUP BY 1
ORDER BY corr_sales DESC
LIMIT 5;

WITH metrics AS (
  SELECT 
    STORE_NBR,
    AVG(total_sales) AS avg_sales,
    AVG(total_customers) AS avg_cust
  FROM `case-studies-478607.quantium_analysis.monthly_store_metrics`
  WHERE month_id < '201902'
  GROUP BY 1
),
abs_diff AS (
  SELECT 
    STORE_NBR,
    ABS(metrics.avg_sales - (SELECT avg_sales FROM metrics WHERE STORE_NBR = 86)) AS sales_diff,
    ABS(metrics.avg_cust - (SELECT avg_cust FROM metrics WHERE STORE_NBR = 86)) AS cust_diff
  FROM metrics
)
SELECT 
  STORE_NBR,
  1 - (sales_diff - MIN(sales_diff) OVER()) / (MAX(sales_diff) OVER() - MIN(sales_diff) OVER()) AS mag_sales,
  1 - (cust_diff - MIN(cust_diff) OVER()) / (MAX(cust_diff) OVER() - MIN(cust_diff) OVER()) AS mag_cust
FROM abs_diff
WHERE STORE_NBR IN (155, 132, 138)
ORDER BY mag_sales DESC;

WITH scaling_factor AS (
  SELECT 
    SUM(CASE WHEN STORE_NBR = 86 THEN total_sales END) / 
    SUM(CASE WHEN STORE_NBR = 155 THEN total_sales END) AS sales_ratio
  FROM `case-studies-478607.quantium_analysis.monthly_store_metrics`
  WHERE month_id < '201902'
)
SELECT 
  month_id,
  MAX(CASE WHEN STORE_NBR = 86 THEN total_sales END) AS trial_sales,
  ROUND(MAX(CASE WHEN STORE_NBR = 155 THEN total_sales END) * (SELECT sales_ratio FROM scaling_factor), 2) AS scaled_control_sales,
  ROUND(((MAX(CASE WHEN STORE_NBR = 86 THEN total_sales END) / (MAX(CASE WHEN STORE_NBR = 155 THEN total_sales END) * (SELECT sales_ratio FROM scaling_factor))) - 1) * 100, 2) AS percentage_uplift
FROM `case-studies-478607.quantium_analysis.monthly_store_metrics`
WHERE month_id BETWEEN '201902' AND '201904'
GROUP BY 1
ORDER BY 1;

WITH trial_store_88 AS (
  SELECT month_id, total_sales, total_customers
  FROM `case-studies-478607.quantium_analysis.monthly_store_metrics`
  WHERE STORE_NBR = 88 AND month_id < '201902'
),
all_other_stores AS (
  SELECT STORE_NBR, month_id, total_sales, total_customers
  FROM `case-studies-478607.quantium_analysis.monthly_store_metrics`
  WHERE STORE_NBR NOT IN (77, 86, 88) AND month_id < '201902'
)
SELECT 
  others.STORE_NBR,
  ROUND(CORR(others.total_sales, trial.total_sales), 4) AS corr_sales,
  ROUND(CORR(others.total_customers, trial.total_customers), 4) AS corr_customers
FROM all_other_stores AS others
JOIN trial_store_88 AS trial ON others.month_id = trial.month_id
GROUP BY 1
ORDER BY corr_sales DESC
LIMIT 5;

WITH metrics AS (
  SELECT 
    STORE_NBR,
    AVG(total_sales) AS avg_sales,
    AVG(total_customers) AS avg_cust
  FROM `case-studies-478607.quantium_analysis.monthly_store_metrics`
  WHERE month_id < '201902'
  GROUP BY 1
),
abs_diff AS (
  SELECT 
    STORE_NBR,
    ABS(metrics.avg_sales - (SELECT avg_sales FROM metrics WHERE STORE_NBR = 88)) AS sales_diff,
    ABS(metrics.avg_cust - (SELECT avg_cust FROM metrics WHERE STORE_NBR = 88)) AS cust_diff
  FROM metrics
)
SELECT 
  STORE_NBR,
  1 - (sales_diff - MIN(sales_diff) OVER()) / (MAX(sales_diff) OVER() - MIN(sales_diff) OVER()) AS mag_sales,
  1 - (cust_diff - MIN(cust_diff) OVER()) / (MAX(cust_diff) OVER() - MIN(cust_diff) OVER()) AS mag_cust
FROM abs_diff
WHERE STORE_NBR IN (159, 91, 204, 1, 240)
ORDER BY mag_sales DESC;

WITH scaling_factor AS (
  SELECT 
    SUM(CASE WHEN STORE_NBR = 88 THEN total_sales END) / 
    SUM(CASE WHEN STORE_NBR = 91 THEN total_sales END) AS sales_ratio
  FROM `case-studies-478607.quantium_analysis.monthly_store_metrics`
  WHERE month_id < '201902'
)
SELECT 
  month_id,
  MAX(CASE WHEN STORE_NBR = 88 THEN total_sales END) AS trial_sales,
  ROUND(MAX(CASE WHEN STORE_NBR = 91 THEN total_sales END) * (SELECT sales_ratio FROM scaling_factor), 2) AS scaled_control_sales,
  ROUND(((MAX(CASE WHEN STORE_NBR = 88 THEN total_sales END) / (MAX(CASE WHEN STORE_NBR = 91 THEN total_sales END) * (SELECT sales_ratio FROM scaling_factor))) - 1) * 100, 2) AS percentage_uplift
FROM `case-studies-478607.quantium_analysis.monthly_store_metrics`
WHERE month_id BETWEEN '201902' AND '201904'
GROUP BY 1
ORDER BY 1;
