# Ecommerce-Analysis-SQL-
--Query 01: calculate total visit, pageview, transaction for Jan, Feb and March 2017 (order by month)
SELECT  FORMAT_DATE("%Y%m",PARSE_DATE("%Y%m%d",date)) AS month,
        SUM(totals.visits) AS visits, 
        SUM(totals.pageviews) AS pageviews,   
        SUM(totals.transactions) AS transactions
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*` 
WHERE FORMAT_DATE("%Y%m",PARSE_DATE("%Y%m%d",date)) BETWEEN '201701' AND '201703' 
-- Co the dung WHERE _table_suffix BETTWEN '01' AND '04'
GROUP BY month 
ORDER BY month;

--Query 02: Bounce rate per traffic source in July 2017 (Bounce_rate = num_bounce/total_visit) (order by total_visit DESC)
SELECT  trafficSource.source AS source,
        SUM(totals.visits) AS total_visits,
        SUM(totals.bounces) AS total_no_of_bounces,
        (SUM(totals.bounces)/SUM(totals.visits)*100) AS bounce_rate
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
GROUP BY trafficSource.source
ORDER BY total_visits DESC;

--Query 3: Revenue by traffic source by week, by month in June 2017
with productRevenue_by_month AS (
SELECT "Month" AS time_type,
        FORMAT_DATE("%Y%m",PARSE_DATE("%Y%m%d",date)) AS time,
        trafficSource.source AS source,
        SUM(productRevenue) AS revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`,
UNNEST (hits) hits,
UNNEST (hits.product) product
WHERE productRevenue IS NOT NULL
GROUP BY time_type, time, source),
productRevenue_by_week AS (
SELECT "Week" AS time_type,
        FORMAT_DATE("%Y%W",PARSE_DATE("%Y%m%d",date)) AS time,
        trafficSource.source AS source,
        SUM(productRevenue) AS revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`,
UNNEST (hits) hits,
UNNEST (hits.product) product
WHERE productRevenue IS NOT NULL
GROUP BY time_type, time, source)

SELECT *
FROM productRevenue_by_month
UNION ALL
SELECT *
FROM productRevenue_by_week
ORDER BY revenue DESC;

--Query 04: Average number of pageviews by purchaser type (purchasers vs non-purchasers) in June, July 2017.
WITH purchaser_data AS(
  SELECT
      FORMAT_DATE("%Y%m",PARSE_DATE("%Y%m%d",DATE)) AS month,
      (SUM(totals.pageviews)/COUNT(DISTINCT fullvisitorid)) AS avg_pageviews_purchase,
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
    ,UNNEST(hits) hits
    ,UNNEST(product) product
  WHERE _table_suffix BETWEEN '0601' AND '0731'
  AND totals.transactions>=1
  --and totals.totalTransactionRevenue is not null
  AND product.productRevenue IS NOT NULL
  GROUP BY month
),

non_purchaser_data AS(
  SELECT
      FORMAT_DATE("%Y%m",PARSE_DATE("%Y%m%d",DATE)) AS month,
      SUM(totals.pageviews)/COUNT(DISTINCT fullvisitorid) AS avg_pageviews_non_purchase,
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
      ,UNNEST(hits) hits
    ,UNNEST(product) product
  WHERE _table_suffix BETWEEN '0601' AND '0731'
  AND totals.transactions IS NULL
  AND product.productRevenue IS NULL
  GROUP BY month
)

SELECT
    pd.*,
    avg_pageviews_non_purchase
FROM purchaser_data pd
LEFT JOIN non_purchaser_data USING(month)
ORDER BY pd.month;


--Query 05: Average number of transactions per user that made a purchase in July 2017
SELECT  FORMAT_DATE ("%Y%m", PARSE_DATE("%Y%m%d",date)) as Month,
        (SUM(totals.transactions)/COUNT(DISTINCT fullVisitorId)) AS Avg_total_transactions_per_user
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*` ,
UNNEST (hits) hits,
UNNEST (hits.product) product
WHERE productRevenue IS NOT NULL
GROUP by Month;

--Query 06: Average amount of money spent per session. Only include purchaser data in July 2017
SELECT  FORMAT_DATE ("%Y%m", PARSE_DATE("%Y%m%d",date)) as Month,
        (SUM(productRevenue)/SUM(totals.visits)) AS avg_revenue_by_user_per_visit
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
UNNEST (hits) hits,
UNNEST (hits.product) product
WHERE totals.transactions IS NOT NULL 
  AND productRevenue IS NOT NULL
GROUP BY Month;

--Query 07: Other products purchased by customers who purchased product "YouTube Men's Vintage Henley" in July 2017. Output should show product name and the quantity was ordered.
with G1 AS(
SELECT  fullVisitorId AS ID,
        v2ProductName AS name,
        SUM(productQuantity) AS quantity
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
UNNEST (hits) hits,
UNNEST (hits.product) product
WHERE v2ProductName = "YouTube Men's Vintage Henley"
        AND productRevenue IS NOT NULL
GROUP BY ID, name),

G2 AS(
SELECT  fullVisitorId AS ID,
        v2ProductName AS other_purchased_products,
        SUM(productQuantity) AS quantity
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
UNNEST (hits) hits,
UNNEST (hits.product) product
WHERE v2ProductName NOT LIKE "YouTube Men's Vintage Henley"
        AND productRevenue IS NOT NULL
GROUP BY ID, other_purchased_products)

SELECT  G2.other_purchased_products,
        SUM(G2.quantity) 
FROM G2
INNER JOIN G1
USING (ID)
GROUP BY 1 
ORDER BY 2 DESC;

--Query 08: Calculate cohort map from product view to addtocart to purchase in Jan, Feb and March 2017. For example, 100% product view then 40% add_to_cart and 10% purchase.
SELECT  FORMAT_DATE ("%Y%m", PARSE_DATE ("%Y%m%d",date)) AS month,
        COUNT(CASE WHEN eCommerceAction.action_type = '2' THEN eCommerceAction.action_type END) AS num_product_new,
        COUNT(CASE WHEN eCommerceAction.action_type = '3' THEN eCommerceAction.action_type END) AS num_addtocart,
        COUNT(CASE WHEN eCommerceAction.action_type = '6' AND productRevenue IS NOT NULL THEN eCommerceAction.action_type END) AS num_purchase,
        ROUND((COUNT(CASE WHEN eCommerceAction.action_type = '3' THEN eCommerceAction.action_type END)/COUNT(CASE WHEN eCommerceAction.action_type = '2' THEN eCommerceAction.action_type END))*100,2) AS add_to_cart_rate,
        ROUND((COUNT(CASE WHEN eCommerceAction.action_type = '6' AND productRevenue IS NOT NULL THEN eCommerceAction.action_type END)/COUNT(CASE WHEN eCommerceAction.action_type = '2' THEN eCommerceAction.action_type END))*100,2) AS purchase_rate
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
UNNEST (hits) hits,
UNNEST (hits.product) product
WHERE FORMAT_DATE("%Y%m", PARSE_DATE ("%Y%m%d",date)) BETWEEN '201701' AND '201703'
GROUP BY month
ORDER BY month;
