--  BRAZIL E-COMMERCE (OLIST) -- EDA QUERIES
--  Run each query in pgAdmin, export result as CSV
--  Database: olist_db
--  NOTE: All date columns cast to ::timestamp (stored as text)


--Q1: Monthly Revenue 
--Save as: monthly_revenue.csv
SELECT
    DATE_TRUNC('month', orders.order_purchase_timestamp::timestamp)      AS month,
    COUNT(DISTINCT orders.order_id)                                       AS total_orders,
    ROUND(SUM(order_items.price + order_items.freight_value)::NUMERIC, 2)              AS total_revenue,
    ROUND(AVG(order_items.price + order_items.freight_value)::NUMERIC, 2)              AS avg_order_value
FROM orders
JOIN order_items 
  ON orders.order_id = order_items.order_id
WHERE orders.order_status = 'delivered'
  AND orders.order_purchase_timestamp::timestamp BETWEEN '2017-01-01' AND '2018-08-31'
GROUP BY 1
ORDER BY 1;


--Q2: Peak Days & Hours
--Save as: peak_days_hours.csv
SELECT
    TO_CHAR(orders.order_purchase_timestamp::timestamp, 'Dy')              AS day_name,
    EXTRACT(HOUR FROM orders.order_purchase_timestamp::timestamp)::INT     AS hour,
    COUNT(*)                                                          AS total_orders
FROM orders
JOIN order_items ON orders.order_id = order_items.order_id
GROUP BY 1, 2
ORDER BY total_orders DESC;


--Q3: Payment Methods
--Save as: payment_methods.csv
SELECT
    payment_type,
    COUNT(DISTINCT order_id)                                     AS total_orders,
    ROUND(SUM(payment_value)::NUMERIC, 2)                        AS total_value,
    ROUND(AVG(payment_installments)::NUMERIC, 1)                 AS avg_installments,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1)           AS pct_share
FROM order_payments
GROUP BY payment_type
ORDER BY total_orders DESC;


--Q4: Customer Segments
--Save as: customer_segments.csv
SELECT
    CASE
        WHEN order_count = 1  THEN 'One-time'
        WHEN order_count <= 3 THEN 'Occasional (2-3)'
        ELSE 'Loyal (4+)'
    END                AS segment,
    COUNT(*)           AS customers,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) AS pct
FROM (
    SELECT customers.customer_unique_id, COUNT(orders.order_id) AS order_count
    FROM customers 
    JOIN orders  ON customers.customer_id = orders.customer_id
    GROUP BY customers.customer_unique_id
) sub
GROUP BY 1
ORDER BY customers DESC;


--Q5: Spend Distribution
--Save as: spend_distribution.csv
SELECT
    ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY total_spend)::NUMERIC, 2) AS p25,
    ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY total_spend)::NUMERIC, 2) AS median,
    ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY total_spend)::NUMERIC, 2) AS p75,
    ROUND(PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY total_spend)::NUMERIC, 2) AS p90,
    ROUND(AVG(total_spend)::NUMERIC, 2)                                           AS mean
FROM (
    SELECT customers.customer_unique_id,
           SUM(order_items.price + order_items.freight_value) AS total_spend
    FROM customers 
    JOIN orders        ON customers.customer_id = orders.customer_id
    JOIN order_items  ON orders.order_id    = order_items.order_id
    WHERE orders.order_status = 'delivered'
    GROUP BY customers.customer_unique_id
) sub;


--Q6: RFM Segmentation
--Save as: rfm.csv
WITH rfm_base AS (
    SELECT
        c.customer_unique_id,
        DATE '2018-10-18' - MAX(orders.order_purchase_timestamp::timestamp)::DATE AS recency_days,
        COUNT(DISTINCT orders.order_id)                                             AS frequency,
        ROUND(SUM(order_items.price + order_items.freight_value)::NUMERIC, 2)                   AS monetary
    FROM customers 
    JOIN orders       ON customers.customer_id = orders.customer_id
    JOIN order_items ON orders.order_id    = order_items.order_id
    WHERE orders.order_status = 'delivered'
    GROUP BY customers.customer_unique_id
),
rfm_scores AS (
    SELECT *,
        NTILE(5) OVER (ORDER BY recency_days DESC) AS r_score,
        NTILE(5) OVER (ORDER BY frequency ASC)     AS f_score,
        NTILE(5) OVER (ORDER BY monetary ASC)      AS m_score
    FROM rfm_base
)
SELECT
    r_score, f_score, m_score,
    CONCAT(r_score, f_score, m_score)  AS rfm_segment,
    COUNT(*)                           AS customers,
    ROUND(AVG(monetary)::NUMERIC, 2)   AS avg_spend
FROM rfm_scores
GROUP BY 1, 2, 3
ORDER BY r_score DESC, f_score DESC, m_score DESC;


--Q7: Top Sellers
--Save as: top_sellers.csv
SELECT
    sellers.seller_id,
    sellers.seller_state,
    COUNT(DISTINCT order_items.order_id)                                      AS total_orders,
    ROUND(SUM(order_items.price)::NUMERIC, 2)                                 AS total_revenue,
    ROUND(AVG(order_reviews.review_score)::NUMERIC, 2)                           AS avg_review_score,
    ROUND(AVG(CASE WHEN orders.order_delivered_customer_date::timestamp
                       > orders.order_estimated_delivery_date::timestamp
                   THEN 1.0 ELSE 0.0 END) * 100, 1) || '%'          AS pct_late
FROM sellers
JOIN order_items   ON sellers.seller_id = order_items.seller_id
JOIN orders        ON order_items.order_id = orders.order_id
JOIN order_reviews ON orders.order_id  = order_reviews.order_id
WHERE orders.order_status = 'delivered'
GROUP BY sellers.seller_id, sellers.seller_state
HAVING COUNT(DISTINCT order_items.order_id) >= 10
ORDER BY total_revenue DESC
LIMIT 20;


--Q8: Late Delivery vs Review Score
--Save as: late_vs_score.csv
SELECT
    CASE
        WHEN pct_late < 5  THEN '0-5%'
        WHEN pct_late < 10 THEN '5-10%'
        WHEN pct_late < 15 THEN '10-15%'
        WHEN pct_late < 20 THEN '15-20%'
        ELSE '20%+'
    END                               AS late_rate_bucket,
    COUNT(*)                          AS sellers,
    ROUND(AVG(avg_score)::NUMERIC, 3) AS avg_review_score
FROM (
    SELECT
        sellers.seller_id,
        ROUND(AVG(order_reviews.review_score)::NUMERIC, 2)                       AS avg_score,
        ROUND(AVG(CASE WHEN orders.order_delivered_customer_date::timestamp
                           > orders.order_estimated_delivery_date::timestamp
                       THEN 100.0 ELSE 0.0 END), 1)                  AS pct_late
    FROM sellers
    JOIN order_items  ON sellers.seller_id = order_items.seller_id
    JOIN orders        ON order_items.order_id = orders.order_id
    JOIN order_reviews ON orders.order_id  = order_reviews.order_id
    WHERE orders.order_status = 'delivered'
    GROUP BY sellers.seller_id
    HAVING COUNT(DISTINCT order_items.order_id) >= 20
) sub
GROUP BY 1
ORDER BY late_rate_bucket;


--Q9: Delivery Stats
--Save as: delivery_stats.csv
SELECT
    COUNT(*)                                                            AS total_orders,
    SUM(CASE WHEN order_delivered_customer_date::timestamp
                  > order_estimated_delivery_date::timestamp
             THEN 1 ELSE 0 END)                                        AS late_orders,
    ROUND(AVG(CASE WHEN order_delivered_customer_date::timestamp
                       > order_estimated_delivery_date::timestamp
                   THEN 1.0 ELSE 0.0 END) * 100, 1)                   AS pct_late,
    ROUND(AVG(EXTRACT(EPOCH FROM (
        order_delivered_customer_date::timestamp - order_purchase_timestamp::timestamp
    )) / 86400)::NUMERIC, 1)                                           AS avg_actual_days,
    ROUND(AVG(EXTRACT(EPOCH FROM (
        order_estimated_delivery_date::timestamp - order_purchase_timestamp::timestamp
    )) / 86400)::NUMERIC, 1)                                           AS avg_estimated_days
FROM orders
WHERE order_status = 'delivered'
  AND order_delivered_customer_date IS NOT NULL
  AND order_estimated_delivery_date IS NOT NULL;


--Q10: Late Delivery by State
--Save as: late_by_state.csv
SELECT
    customers.customer_state,
    COUNT(*)                                                            AS total_orders,
    ROUND(AVG(CASE WHEN orders.order_delivered_customer_date::timestamp
                       > orders.order_estimated_delivery_date::timestamp
                   THEN 1.0 ELSE 0.0 END) * 100, 1)                   AS pct_late,
    ROUND(AVG(EXTRACT(EPOCH FROM (
        orders.order_delivered_customer_date::timestamp - orders.order_purchase_timestamp::timestamp
    )) / 86400)::NUMERIC, 1)                                           AS avg_delivery_days
FROM orders
JOIN customers ON orders.customer_id = customers.customer_id
WHERE orders.order_status = 'delivered'
  AND orders.order_delivered_customer_date IS NOT NULL
GROUP BY customers.customer_state
HAVING COUNT(*) >= 100
ORDER BY pct_late DESC;


--Q11: Category Revenue
--Save as:categories_revenue.csv
SELECT
    COALESCE(products.product_category_name_english,
             products.product_category_name)            AS category,
    COUNT(DISTINCT order_items.order_id)                  AS total_orders,
    ROUND(SUM(order_items.price)::NUMERIC, 2)             AS total_revenue,
    ROUND(AVG(order_items.price)::NUMERIC, 2)             AS avg_price,
    ROUND(AVG(order_reviews.review_score)::NUMERIC, 2)       AS avg_review_score
FROM products
JOIN order_items   ON products.product_id = order_items.product_id
JOIN orders        ON order_items.order_id  = orders.order_id
JOIN order_reviews ON orders.order_id   = order_reviews.order_id
WHERE orders.order_status = 'delivered'
GROUP BY 1
HAVING COUNT(DISTINCT order_items.order_id) >= 50
ORDER BY total_revenue DESC
LIMIT 15;


--Q12: Price Tiers
--Save as: price_tiers.csv
SELECT
    CASE
        WHEN order_items.price < 50  THEN 'Budget (< R$50)'
        WHEN order_items.price < 150 THEN 'Mid (R$50-150)'
        WHEN order_items.price < 500 THEN 'Premium (R$150-500)'
        ELSE 'Luxury (R$500+)'
    END                                                            AS price_tier,
    COUNT(DISTINCT order_items.order_id)                                    AS total_orders,
    ROUND(AVG(order_reviews.review_score)::NUMERIC, 2)                         AS avg_score,
    ROUND(AVG(CASE WHEN orders.order_delivered_customer_date::timestamp
                       > orders.order_estimated_delivery_date::timestamp
                   THEN 1.0 ELSE 0.0 END) * 100, 1)               AS pct_late
FROM order_items
JOIN orders        ON order_items.order_id = orders.order_id
JOIN order_reviews r ON orders.order_id  = order_reviews.order_id
WHERE orders.order_status = 'delivered'
GROUP BY 1
ORDER BY 1;


--Q13: Revenue by State
--Save as: geo_revenue.csv
SELECT
    customers.customer_state,
    COUNT(DISTINCT orders.order_id)                                     AS total_orders,
    ROUND(SUM(order_items.price + order_items.freight_value)::NUMERIC, 2)            AS total_revenue,
    ROUND(AVG(order_items.price + order_items.freight_value)::NUMERIC, 2)            AS avg_order_value,
    ROUND(AVG(order_reviews.review_score)::NUMERIC, 2)                         AS avg_score
FROM customers
JOIN orders        ON customers.customer_id = orders.customer_id
JOIN order_items   ON orders.order_id    = order_items.order_id
JOIN order_reviews ON orders.order_id    = order_reviews.order_id
WHERE orders.order_status = 'delivered'
GROUP BY customers.customer_state
ORDER BY total_revenue DESC;


--Q14: Cross-State Flow
--Save as: cross_state.csv
SELECT
    sellers.seller_state,
    customers.customer_state,
    COUNT(DISTINCT orders.order_id)               AS total_orders,
    ROUND(SUM(order_items.freight_value)::NUMERIC, 2) AS total_freight
FROM orders
JOIN customers      ON orders.customer_id = customers.customer_id
JOIN order_items   ON orders.order_id    = order_items.order_id
JOIN sellers       ON order_items.seller_id  = sellers.seller_id
WHERE orders.order_status = 'delivered'
GROUP BY sellers.seller_state, customers.customer_state
ORDER BY total_orders DESC
LIMIT 20;



--Q15: Cumulative Revenue
--Save as: cumulative_revenue.csv
SELECT
    DATE_TRUNC('month', orders.order_purchase_timestamp::timestamp)       AS month,
    ROUND(SUM(order_items.price + order_items.freight_value)::NUMERIC, 2)              AS monthly_revenue,
    ROUND(SUM(SUM(order_items.price + order_items.freight_value))
          OVER (ORDER BY DATE_TRUNC('month', orders.order_purchase_timestamp::timestamp))
          ::NUMERIC, 2)                                              AS cumulative_revenue,
    COUNT(DISTINCT orders.order_id)                                        AS monthly_orders,
    SUM(COUNT(DISTINCT orders.order_id))
        OVER (ORDER BY DATE_TRUNC('month', orders.order_purchase_timestamp::timestamp)) AS cumulative_orders
FROM orders
JOIN order_items  ON orders.order_id = order_items.order_id
WHERE orders.order_status = 'delivered'
  AND orders.order_purchase_timestamp::timestamp BETWEEN '2017-01-01' AND '2018-08-31'
GROUP BY 1
ORDER BY 1;


--Q16: Seller Ranking 
--Save as: seller_ranking.csv
SELECT
    sellers.seller_id,
    sellers.seller_state,
    sellers.seller_city,
    COUNT(DISTINCT order_items.order_id)                                      AS total_orders,
    ROUND(SUM(order_items.price)::NUMERIC, 2)                                 AS total_revenue,
    ROUND(AVG(order_reviews.review_score)::NUMERIC, 2)                           AS avg_review_score,
    ROUND(AVG(CASE WHEN orders.order_delivered_customer_date::timestamp
                       > orders.order_estimated_delivery_date::timestamp
                   THEN 1.0 ELSE 0.0 END) * 100, 1)                 AS pct_late,
    RANK()       OVER (ORDER BY SUM(order_items.price) DESC)                  AS revenue_rank,
    RANK()       OVER (ORDER BY AVG(order_reviews.review_score) DESC)            AS score_rank,
    RANK()       OVER (ORDER BY COUNT(DISTINCT order_items.order_id) DESC)    AS orders_rank,
    DENSE_RANK() OVER (PARTITION BY sellers.seller_state
                       ORDER BY SUM(order_items.price) DESC)                  AS revenue_rank_in_state,
    CASE
        WHEN RANK() OVER (ORDER BY SUM(order_items.price) DESC) <= 10  THEN 'Top 10'
        WHEN RANK() OVER (ORDER BY SUM(order_items.price) DESC) <= 50  THEN 'Top 50'
        WHEN RANK() OVER (ORDER BY SUM(order_items.price) DESC) <= 100 THEN 'Top 100'
        ELSE 'Others'
    END                                                              AS performance_tier
FROM sellers
JOIN order_items   ON sellers.seller_id = order_items.seller_id
JOIN orders        ON order_items.order_id = orders.order_id
JOIN order_reviews  ON orders.order_id  = order_reviews.order_id
WHERE orders.order_status = 'delivered'
GROUP BY sellers.seller_id, sellers.seller_state, sellers.seller_city
HAVING COUNT(DISTINCT order_items.order_id) >= 10
ORDER BY revenue_rank;