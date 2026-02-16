-- ============================================================================
-- FILE: 03_kpi_queries/02_customer_analytics.sql
-- PROJECT: RetailMart Enterprise Analytics Platform
-- PURPOSE: Customer Analytics Module - Complete customer behavior tracking
-- AUTHOR: SQL Bootcamp
-- CREATED: 2025
--
-- DESCRIPTION:
--   "Your most unhappy customers are your greatest source of learning" - Bill Gates
--   
--   This module helps answer:
--   - Who are our most valuable customers? (CLV Analysis)
--   - How should we segment customers? (RFM Analysis)
--   - Are we retaining customers? (Cohort Retention)
--   - Who is about to leave? (Churn Prediction)
--   - What do our customers look like? (Demographics)
--
--   Real-world example: Swiggy spends ₹600 to acquire a customer.
--   They NEED to know if CLV > ₹600, otherwise they're losing money!
--
-- CREATES:
--   • 4 Regular Views
--   • 3 Materialized Views
--   • 6 JSON Export Functions
--
-- EXECUTION ORDER: Run AFTER 01_sales_analytics.sql
-- ============================================================================

\echo ''
\echo '============================================================================'
\echo '             CUSTOMER ANALYTICS MODULE - STARTING                           '
\echo '============================================================================'
\echo ''

-- ============================================================================
-- MATERIALIZED VIEW 1: CUSTOMER LIFETIME VALUE (CLV)
-- ============================================================================
-- Purpose: Calculate comprehensive customer value and segment into tiers
-- Business Impact: Determines how much to spend on acquisition/retention
-- ============================================================================

\echo '[1/7] Creating materialized view: mv_customer_lifetime_value...'

DROP MATERIALIZED VIEW IF EXISTS analytics.mv_customer_lifetime_value CASCADE;

CREATE MATERIALIZED VIEW analytics.mv_customer_lifetime_value AS
WITH customer_orders AS (
    SELECT 
        c.cust_id,
        c.full_name,
        c.gender,
        c.age,
        c.city,
        c.state,
        c.region_name,
        c.join_date,
        
        -- Order Metrics
        COUNT(DISTINCT o.order_id) as total_orders,
        SUM(o.total_amount) as total_revenue,
        AVG(o.total_amount) as avg_order_value,
        
        -- Timeline
        MIN(o.order_date) as first_order_date,
        MAX(o.order_date) as last_order_date,
        (SELECT MAX(order_date) FROM sales.orders) - MAX(o.order_date) as days_since_last_order,
        MAX(o.order_date) - MIN(o.order_date) as customer_lifespan_days,
        
        -- Items
        SUM(oi.quantity) as total_items_purchased
        
    FROM customers.customers c
    LEFT JOIN sales.orders o ON c.cust_id = o.cust_id AND o.order_status = 'Delivered'
    LEFT JOIN sales.order_items oi ON o.order_id = oi.order_id
    GROUP BY c.cust_id, c.full_name, c.gender, c.age, c.city, c.state, c.region_name, c.join_date
),
customer_loyalty AS (
    SELECT cust_id, total_points FROM customers.loyalty_points
),
customer_reviews AS (
    SELECT cust_id, COUNT(*) as review_count, ROUND(AVG(rating), 2) as avg_rating_given
    FROM customers.reviews
    GROUP BY cust_id
)
SELECT 
    co.cust_id,
    co.full_name,
    co.gender,
    co.age,
    co.city,
    co.state,
    co.region_name,
    co.join_date,
    
    -- Order Metrics
    COALESCE(co.total_orders, 0) as total_orders,
    ROUND(COALESCE(co.total_revenue, 0)::NUMERIC, 2) as total_revenue,
    ROUND(COALESCE(co.avg_order_value, 0)::NUMERIC, 2) as avg_order_value,
    COALESCE(co.total_items_purchased, 0) as total_items_purchased,
    
    -- Timeline
    co.first_order_date,
    co.last_order_date,
    COALESCE(co.days_since_last_order, 9999) as days_since_last_order,
    COALESCE(co.customer_lifespan_days, 0) as customer_lifespan_days,
    
    -- Loyalty & Engagement
    COALESCE(cl.total_points, 0) as loyalty_points,
    COALESCE(cr.review_count, 0) as review_count,
    COALESCE(cr.avg_rating_given, 0) as avg_rating_given,
    
    -- Calculated Metrics
    ROUND(
        COALESCE(co.total_revenue, 0) / NULLIF(GREATEST(co.customer_lifespan_days, 1), 0) * 365,
        2
    )::NUMERIC as projected_annual_value,
    
    ROUND(
        COALESCE(co.total_orders, 0)::NUMERIC / NULLIF(GREATEST(co.customer_lifespan_days, 1), 0) * 30,
        2
    ) as avg_orders_per_month,
    
    -- CLV Tier (using config values)
    CASE 
        WHEN COALESCE(co.total_revenue, 0) >= (SELECT analytics.get_config_number('clv_tier_platinum')) THEN 'Platinum'
        WHEN COALESCE(co.total_revenue, 0) >= (SELECT analytics.get_config_number('clv_tier_gold')) THEN 'Gold'
        WHEN COALESCE(co.total_revenue, 0) >= (SELECT analytics.get_config_number('clv_tier_silver')) THEN 'Silver'
        WHEN COALESCE(co.total_revenue, 0) >= (SELECT analytics.get_config_number('clv_tier_bronze')) THEN 'Bronze'
        ELSE 'Basic'
    END as clv_tier,
    
    -- Customer Status
    CASE 
        WHEN co.total_orders IS NULL OR co.total_orders = 0 THEN 'Never Purchased'
        WHEN co.days_since_last_order <= (SELECT analytics.get_config_number('rfm_recency_active_days')) THEN 'Active'
        WHEN co.days_since_last_order <= (SELECT analytics.get_config_number('rfm_recency_at_risk_days')) THEN 'At Risk'
        WHEN co.days_since_last_order <= (SELECT analytics.get_config_number('rfm_recency_churning_days')) THEN 'Churning'
        ELSE 'Churned'
    END as customer_status,
    
    -- Age Group
    CASE 
        WHEN co.age < 25 THEN '18-24'
        WHEN co.age < 35 THEN '25-34'
        WHEN co.age < 45 THEN '35-44'
        WHEN co.age < 55 THEN '45-54'
        ELSE '55+'
    END as age_group

FROM customer_orders co
LEFT JOIN customer_loyalty cl ON co.cust_id = cl.cust_id
LEFT JOIN customer_reviews cr ON co.cust_id = cr.cust_id;

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_clv_tier ON analytics.mv_customer_lifetime_value(clv_tier);
CREATE INDEX IF NOT EXISTS idx_clv_status ON analytics.mv_customer_lifetime_value(customer_status);
CREATE INDEX IF NOT EXISTS idx_clv_region ON analytics.mv_customer_lifetime_value(region_name);

COMMENT ON MATERIALIZED VIEW analytics.mv_customer_lifetime_value IS 
    'Customer Lifetime Value with tier classification - Refresh daily';

\echo '      ✓ Materialized view created: mv_customer_lifetime_value'


-- ============================================================================
-- MATERIALIZED VIEW 2: RFM ANALYSIS
-- ============================================================================
-- Purpose: Segment customers by Recency, Frequency, Monetary value
-- Use Case: Targeted marketing, personalized campaigns
-- 
-- RFM Scoring: Each dimension scored 1-5 (5 = best)
-- - Recency: How recently did they purchase? (Lower days = Higher score)
-- - Frequency: How often do they purchase? (More orders = Higher score)
-- - Monetary: How much do they spend? (Higher spend = Higher score)
-- ============================================================================

\echo '[2/7] Creating materialized view: mv_rfm_analysis...'

DROP MATERIALIZED VIEW IF EXISTS analytics.mv_rfm_analysis CASCADE;

CREATE MATERIALIZED VIEW analytics.mv_rfm_analysis AS
WITH customer_rfm AS (
    SELECT 
        c.cust_id,
        c.full_name,
        c.city,
        c.state,
        (SELECT MAX(order_date) FROM sales.orders) - MAX(o.order_date) as recency_days,
        COUNT(DISTINCT o.order_id) as frequency,
        SUM(o.total_amount) as monetary
    FROM customers.customers c
    JOIN sales.orders o ON c.cust_id = o.cust_id AND o.order_status = 'Delivered'
    GROUP BY c.cust_id, c.full_name, c.city, c.state
),
rfm_scores AS (
    SELECT 
        *,
        -- Score 1-5 using NTILE (quintiles)
        NTILE(5) OVER (ORDER BY recency_days DESC) as r_score,  -- Lower days = Higher score
        NTILE(5) OVER (ORDER BY frequency ASC) as f_score,      -- More orders = Higher score
        NTILE(5) OVER (ORDER BY monetary ASC) as m_score        -- Higher spend = Higher score
    FROM customer_rfm
    WHERE frequency > 0  -- Only customers with purchases
)
SELECT 
    cust_id,
    full_name,
    city,
    state,
    
    -- Raw Metrics
    recency_days,
    frequency as order_count,
    ROUND(monetary::NUMERIC, 2) as total_spent,
    
    -- RFM Scores (1-5)
    r_score as recency_score,
    f_score as frequency_score,
    m_score as monetary_score,
    
    -- Combined RFM Score
    CONCAT(r_score, f_score, m_score) as rfm_score,
    r_score + f_score + m_score as rfm_total,
    
    -- Customer Segment (based on RFM combination)
    CASE
        WHEN r_score >= 4 AND f_score >= 4 AND m_score >= 4 THEN 'Champions'
        WHEN r_score >= 4 AND f_score >= 3 AND m_score >= 3 THEN 'Loyal Customers'
        WHEN r_score >= 4 AND f_score <= 2 AND m_score <= 2 THEN 'Recent Customers'
        WHEN r_score >= 3 AND f_score >= 3 AND m_score >= 4 THEN 'Big Spenders'
        WHEN r_score <= 2 AND f_score >= 4 AND m_score >= 4 THEN 'At Risk - High Value'
        WHEN r_score <= 2 AND f_score >= 3 AND m_score >= 3 THEN 'At Risk'
        WHEN r_score <= 2 AND f_score <= 2 AND m_score >= 3 THEN 'Hibernating'
        WHEN r_score <= 2 AND f_score <= 2 AND m_score <= 2 THEN 'Lost'
        ELSE 'Potential Loyalists'
    END as rfm_segment,
    
    -- Recommended Action
    CASE
        WHEN r_score >= 4 AND f_score >= 4 THEN 'Reward - Exclusive offers & early access'
        WHEN r_score >= 4 AND f_score <= 2 THEN 'Nurture - Onboarding, product education'
        WHEN r_score <= 2 AND f_score >= 3 THEN 'Win Back - Special discount, reach out'
        WHEN r_score <= 2 AND f_score <= 2 AND m_score >= 3 THEN 'Reactivate - Strong offer to return'
        WHEN r_score <= 2 AND f_score <= 2 THEN 'Last Chance - Deep discount or let go'
        ELSE 'Engage - Regular communication'
    END as recommended_action

FROM rfm_scores;

-- Create index on segment for quick filtering
CREATE INDEX IF NOT EXISTS idx_rfm_segment ON analytics.mv_rfm_analysis(rfm_segment);

COMMENT ON MATERIALIZED VIEW analytics.mv_rfm_analysis IS 
    'RFM segmentation for targeted marketing - Refresh weekly';

\echo '      ✓ Materialized view created: mv_rfm_analysis'


-- ============================================================================
-- MATERIALIZED VIEW 3: COHORT RETENTION
-- ============================================================================
-- Purpose: Track how well we retain customers over time
-- Use Case: Measure product-market fit, identify retention issues
-- 
-- A cohort is a group of customers who made their first purchase in the same month.
-- We track what % of each cohort returns in subsequent months.
-- ============================================================================

\echo '[3/7] Creating materialized view: mv_cohort_retention...'

DROP MATERIALIZED VIEW IF EXISTS analytics.mv_cohort_retention CASCADE;

CREATE MATERIALIZED VIEW analytics.mv_cohort_retention AS
WITH customer_first_order AS (
    -- Get each customer's first order month (their cohort)
    SELECT 
        cust_id,
        DATE_TRUNC('month', MIN(order_date))::DATE as cohort_month
    FROM sales.orders
    WHERE order_status = 'Delivered'
    GROUP BY cust_id
),
customer_activity AS (
    -- Get all months each customer was active
    SELECT DISTINCT
        o.cust_id,
        DATE_TRUNC('month', o.order_date)::DATE as activity_month
    FROM sales.orders o
    WHERE o.order_status = 'Delivered'
),
cohort_data AS (
    -- Combine cohort with activity
    SELECT 
        cfo.cohort_month,
        ca.activity_month,
        -- Calculate months since cohort (0 = first month)
        EXTRACT(YEAR FROM AGE(ca.activity_month, cfo.cohort_month)) * 12 +
        EXTRACT(MONTH FROM AGE(ca.activity_month, cfo.cohort_month)) as months_since_cohort,
        COUNT(DISTINCT cfo.cust_id) as customer_count
    FROM customer_first_order cfo
    JOIN customer_activity ca ON cfo.cust_id = ca.cust_id
    GROUP BY cfo.cohort_month, ca.activity_month
),
cohort_sizes AS (
    -- Get initial cohort sizes
    SELECT 
        cohort_month,
        COUNT(DISTINCT cust_id) as cohort_size
    FROM customer_first_order
    GROUP BY cohort_month
)
SELECT 
    cd.cohort_month,
    TO_CHAR(cd.cohort_month, 'Mon YYYY') as cohort_name,
    cs.cohort_size,
    cd.months_since_cohort as month_number,
    cd.customer_count as retained_customers,
    ROUND((cd.customer_count::NUMERIC / cs.cohort_size * 100), 2) as retention_rate
FROM cohort_data cd
JOIN cohort_sizes cs ON cd.cohort_month = cs.cohort_month
WHERE cd.months_since_cohort <= 12  -- Track up to 12 months
ORDER BY cd.cohort_month DESC, cd.months_since_cohort;

-- Create index for quick cohort lookups
CREATE INDEX IF NOT EXISTS idx_cohort_month ON analytics.mv_cohort_retention(cohort_month);

COMMENT ON MATERIALIZED VIEW analytics.mv_cohort_retention IS 
    'Monthly cohort retention analysis - Refresh weekly';

\echo '      ✓ Materialized view created: mv_cohort_retention'


-- ============================================================================
-- VIEW 1: CHURN RISK CUSTOMERS
-- ============================================================================
-- Purpose: Identify high-value customers at risk of churning
-- Use Case: Proactive retention campaigns
-- ============================================================================

\echo '[4/7] Creating view: vw_churn_risk_customers...'

CREATE OR REPLACE VIEW analytics.vw_churn_risk_customers AS
SELECT 
    cust_id,
    full_name,
    city,
    state,
    clv_tier,
    total_orders,
    total_revenue as total_spent,
    days_since_last_order as days_inactive,
    
    -- Churn Risk Level
    CASE 
        WHEN days_since_last_order > 180 THEN 'Churned'
        WHEN days_since_last_order > 90 THEN 'High Risk'
        WHEN days_since_last_order > 60 THEN 'Medium Risk'
        WHEN days_since_last_order > 30 THEN 'Low Risk'
        ELSE 'Active'
    END as churn_risk_level,
    
    -- Priority Score (Higher = More urgent to retain)
    -- High value + Long inactive = Highest priority
    CASE 
        WHEN clv_tier = 'Platinum' THEN 5
        WHEN clv_tier = 'Gold' THEN 4
        WHEN clv_tier = 'Silver' THEN 3
        WHEN clv_tier = 'Bronze' THEN 2
        ELSE 1
    END +
    CASE 
        WHEN days_since_last_order > 90 THEN 5
        WHEN days_since_last_order > 60 THEN 3
        WHEN days_since_last_order > 30 THEN 1
        ELSE 0
    END as priority_score,
    
    -- Recommended Action
    CASE 
        WHEN clv_tier IN ('Platinum', 'Gold') AND days_since_last_order > 60 
            THEN 'URGENT: Personal outreach from account manager'
        WHEN clv_tier IN ('Platinum', 'Gold') AND days_since_last_order > 30 
            THEN 'HIGH: Send exclusive offer + loyalty bonus'
        WHEN days_since_last_order > 90 
            THEN 'Win-back campaign with significant discount'
        WHEN days_since_last_order > 60 
            THEN 'Re-engagement email with personalized recommendations'
        WHEN days_since_last_order > 30 
            THEN 'Reminder email with what''s new'
        ELSE 'No action needed'
    END as recommended_action

FROM analytics.mv_customer_lifetime_value
WHERE total_orders > 0
AND days_since_last_order > 30  -- Focus on at-risk customers
ORDER BY priority_score DESC, total_revenue DESC;

COMMENT ON VIEW analytics.vw_churn_risk_customers IS 'High-value customers at risk of churning';

\echo '      ✓ View created: vw_churn_risk_customers'


-- ============================================================================
-- VIEW 2: CUSTOMER DEMOGRAPHICS
-- ============================================================================
-- Purpose: Understand customer base composition
-- Use Case: Marketing targeting, product development
-- ============================================================================

\echo '[5/7] Creating view: vw_customer_demographics...'

CREATE OR REPLACE VIEW analytics.vw_customer_demographics AS
SELECT 
    age_group,
    gender,
    COUNT(*) as customer_count,
    SUM(total_revenue) as total_revenue,
    ROUND(AVG(total_revenue)::NUMERIC, 2) as avg_revenue_per_customer,
    ROUND(AVG(total_orders)::NUMERIC, 1) as avg_orders_per_customer,
    
    -- Percentage calculations
    ROUND(
        (COUNT(*)::NUMERIC / SUM(COUNT(*)) OVER () * 100), 
        2
    ) as pct_of_customers,
    ROUND(
        (SUM(total_revenue) / SUM(SUM(total_revenue)) OVER () * 100)::NUMERIC, 
        2
    ) as pct_of_revenue

FROM analytics.mv_customer_lifetime_value
WHERE total_orders > 0
GROUP BY age_group, gender
ORDER BY total_revenue DESC;

COMMENT ON VIEW analytics.vw_customer_demographics IS 'Customer breakdown by age group and gender';

\echo '      ✓ View created: vw_customer_demographics'


-- ============================================================================
-- VIEW 3: CUSTOMER GEOGRAPHY
-- ============================================================================
-- Purpose: Geographic distribution of customers
-- Use Case: Store expansion, regional marketing
-- ============================================================================

\echo '[6/7] Creating view: vw_customer_geography...'

CREATE OR REPLACE VIEW analytics.vw_customer_geography AS
WITH geo_stats AS (
    SELECT 
        state,
        city,
        COUNT(*) as customer_count,
        SUM(total_orders) as total_orders,
        SUM(total_revenue) as total_revenue,
        AVG(total_revenue) as avg_revenue_per_customer,
        AVG(avg_order_value) as avg_order_value
    FROM analytics.mv_customer_lifetime_value
    WHERE total_orders > 0
    GROUP BY state, city
)
SELECT 
    state,
    city,
    customer_count,
    total_orders,
    ROUND(total_revenue::NUMERIC, 2) as total_revenue,
    ROUND(avg_order_value::NUMERIC, 2) as avg_order_value,
    ROUND(avg_revenue_per_customer::NUMERIC, 2) as revenue_per_customer,
    RANK() OVER (ORDER BY total_revenue DESC) as revenue_rank,
    RANK() OVER (PARTITION BY state ORDER BY total_revenue DESC) as state_rank
FROM geo_stats
ORDER BY total_revenue DESC;

COMMENT ON VIEW analytics.vw_customer_geography IS 'Customer distribution by location';

\echo '      ✓ View created: vw_customer_geography'


-- ============================================================================
-- VIEW 4: NEW VS RETURNING CUSTOMERS
-- ============================================================================
-- Purpose: Track customer acquisition vs retention
-- Use Case: Balance acquisition and retention spend
-- ============================================================================

\echo '[7/7] Creating view: vw_new_vs_returning...'

CREATE OR REPLACE VIEW analytics.vw_new_vs_returning AS
WITH customer_first_order AS (
    SELECT cust_id, MIN(order_date) as first_order_date
    FROM sales.orders
    WHERE order_status = 'Delivered'
    GROUP BY cust_id
),
monthly_breakdown AS (
    SELECT 
        DATE_TRUNC('month', o.order_date)::DATE as order_month,
        COUNT(DISTINCT o.order_id) as total_orders,
        SUM(o.total_amount) as total_revenue,
        COUNT(DISTINCT o.cust_id) as total_customers,
        COUNT(DISTINCT o.cust_id) FILTER (
            WHERE DATE_TRUNC('month', o.order_date) = DATE_TRUNC('month', cfo.first_order_date)
        ) as new_customers,
        COUNT(DISTINCT o.cust_id) FILTER (
            WHERE DATE_TRUNC('month', o.order_date) > DATE_TRUNC('month', cfo.first_order_date)
        ) as returning_customers
    FROM sales.orders o
    JOIN customer_first_order cfo ON o.cust_id = cfo.cust_id
    WHERE o.order_status = 'Delivered'
    GROUP BY DATE_TRUNC('month', o.order_date)
)
SELECT 
    order_month,
    TO_CHAR(order_month, 'Mon YYYY') as month_name,
    total_orders,
    ROUND(total_revenue::NUMERIC, 2) as total_revenue,
    total_customers,
    new_customers,
    returning_customers,
    ROUND((new_customers::NUMERIC / NULLIF(total_customers, 0) * 100), 2) as new_customer_pct,
    ROUND((returning_customers::NUMERIC / NULLIF(total_customers, 0) * 100), 2) as returning_customer_pct
FROM monthly_breakdown
ORDER BY order_month DESC;

COMMENT ON VIEW analytics.vw_new_vs_returning IS 'New vs returning customer breakdown by month';

\echo '      ✓ View created: vw_new_vs_returning'


-- ============================================================================
-- JSON EXPORT FUNCTIONS
-- ============================================================================

\echo ''
\echo 'Creating JSON export functions...'

-- JSON 1: Top Customers (Top 50 by CLV)
CREATE OR REPLACE FUNCTION analytics.get_top_customers_json()
RETURNS JSON AS $$
BEGIN
    RETURN (
        SELECT json_agg(
            json_build_object(
                'custId', cust_id,
                'fullName', full_name,
                'city', city,
                'state', state,
                'clvTier', clv_tier,
                'totalOrders', total_orders,
                'totalRevenue', total_revenue,
                'avgOrderValue', avg_order_value,
                'daysSinceLastOrder', days_since_last_order,
                'customerStatus', customer_status,
                'loyaltyPoints', loyalty_points
            ) ORDER BY total_revenue DESC
        )
        FROM analytics.mv_customer_lifetime_value
        WHERE total_orders > 0
        LIMIT 50
    );
END;
$$ LANGUAGE plpgsql STABLE;

-- JSON 2: CLV Tier Distribution
CREATE OR REPLACE FUNCTION analytics.get_clv_tier_distribution_json()
RETURNS JSON AS $$
BEGIN
    RETURN (
        SELECT json_agg(
            json_build_object(
                'tier', clv_tier,
                'customerCount', customer_count,
                'totalRevenue', total_revenue,
                'avgRevenue', avg_revenue,
                'pctOfCustomers', pct_of_customers,
                'pctOfRevenue', pct_of_revenue
            ) ORDER BY 
                CASE clv_tier 
                    WHEN 'Platinum' THEN 1 
                    WHEN 'Gold' THEN 2 
                    WHEN 'Silver' THEN 3 
                    WHEN 'Bronze' THEN 4 
                    ELSE 5 
                END
        )
        FROM (
            SELECT 
                clv_tier,
                COUNT(*) as customer_count,
                ROUND(SUM(total_revenue)::NUMERIC, 2) as total_revenue,
                ROUND(AVG(total_revenue)::NUMERIC, 2) as avg_revenue,
                ROUND((COUNT(*)::NUMERIC / SUM(COUNT(*)) OVER () * 100), 2) as pct_of_customers,
                ROUND((SUM(total_revenue) / SUM(SUM(total_revenue)) OVER () * 100)::NUMERIC, 2) as pct_of_revenue
            FROM analytics.mv_customer_lifetime_value
            WHERE total_orders > 0
            GROUP BY clv_tier
        ) tier_stats
    );
END;
$$ LANGUAGE plpgsql STABLE;

-- JSON 3: RFM Segments
CREATE OR REPLACE FUNCTION analytics.get_rfm_segments_json()
RETURNS JSON AS $$
BEGIN
    RETURN (
        SELECT json_agg(
            json_build_object(
                'segment', rfm_segment,
                'customerCount', customer_count,
                'totalRevenue', total_revenue,
                'avgRecencyDays', avg_recency,
                'avgFrequency', avg_frequency,
                'avgMonetary', avg_monetary,
                'recommendedAction', recommended_action
            ) ORDER BY total_revenue DESC
        )
        FROM (
            SELECT 
                rfm_segment,
                COUNT(*) as customer_count,
                ROUND(SUM(total_spent)::NUMERIC, 2) as total_revenue,
                ROUND(AVG(recency_days)::NUMERIC, 0) as avg_recency,
                ROUND(AVG(order_count)::NUMERIC, 1) as avg_frequency,
                ROUND(AVG(total_spent)::NUMERIC, 2) as avg_monetary,
                MAX(recommended_action) as recommended_action
            FROM analytics.mv_rfm_analysis
            GROUP BY rfm_segment
        ) segment_stats
    );
END;
$$ LANGUAGE plpgsql STABLE;

-- JSON 4: Churn Risk
CREATE OR REPLACE FUNCTION analytics.get_churn_risk_json()
RETURNS JSON AS $$
BEGIN
    RETURN (
        SELECT json_build_object(
            'distribution', (
                SELECT json_agg(
                    json_build_object(
                        'riskLevel', churn_risk_level,
                        'customerCount', customer_count,
                        'totalValue', total_value,
                        'avgDaysInactive', avg_days_inactive,
                        'pctOfCustomers', pct_of_customers
                    ) ORDER BY 
                        CASE churn_risk_level 
                            WHEN 'Churned' THEN 1 
                            WHEN 'High Risk' THEN 2 
                            WHEN 'Medium Risk' THEN 3 
                            WHEN 'Low Risk' THEN 4 
                            ELSE 5 
                        END
                )
                FROM (
                    SELECT 
                        churn_risk_level,
                        COUNT(*) as customer_count,
                        ROUND(SUM(total_spent)::NUMERIC, 2) as total_value,
                        ROUND(AVG(days_inactive)::NUMERIC, 0) as avg_days_inactive,
                        ROUND((COUNT(*)::NUMERIC / SUM(COUNT(*)) OVER () * 100), 2) as pct_of_customers
                    FROM analytics.vw_churn_risk_customers
                    GROUP BY churn_risk_level
                ) dist
            ),
            'highPriorityCustomers', (
                SELECT json_agg(
                    json_build_object(
                        'custId', cust_id,
                        'fullName', full_name,
                        'clvTier', clv_tier,
                        'totalSpent', total_spent,
                        'daysInactive', days_inactive,
                        'recommendedAction', recommended_action
                    ) ORDER BY priority_score DESC
                )
                FROM analytics.vw_churn_risk_customers
                WHERE priority_score >= 7
                LIMIT 20
            )
        )
    );
END;
$$ LANGUAGE plpgsql STABLE;

-- JSON 5: Demographics
CREATE OR REPLACE FUNCTION analytics.get_demographics_json()
RETURNS JSON AS $$
BEGIN
    RETURN (
        SELECT json_agg(
            json_build_object(
                'ageGroup', age_group,
                'gender', gender,
                'customerCount', customer_count,
                'totalRevenue', total_revenue,
                'avgRevenue', avg_revenue_per_customer,
                'pctOfCustomers', pct_of_customers,
                'pctOfRevenue', pct_of_revenue
            ) ORDER BY total_revenue DESC
        )
        FROM analytics.vw_customer_demographics
    );
END;
$$ LANGUAGE plpgsql STABLE;

-- JSON 6: Geography (Top 50)
CREATE OR REPLACE FUNCTION analytics.get_geography_json()
RETURNS JSON AS $$
BEGIN
    RETURN (
        SELECT json_agg(
            json_build_object(
                'state', state,
                'city', city,
                'customerCount', customer_count,
                'totalOrders', total_orders,
                'totalRevenue', total_revenue,
                'avgOrderValue', avg_order_value,
                'revenuePerCustomer', revenue_per_customer,
                'rank', revenue_rank
            ) ORDER BY revenue_rank
        )
        FROM analytics.vw_customer_geography
        LIMIT 50
    );
END;
$$ LANGUAGE plpgsql STABLE;

\echo '      ✓ JSON functions created (6 functions)'


-- ============================================================================
-- REFRESH MATERIALIZED VIEWS
-- ============================================================================

\echo ''
\echo 'Refreshing materialized views...'

REFRESH MATERIALIZED VIEW analytics.mv_customer_lifetime_value;
REFRESH MATERIALIZED VIEW analytics.mv_rfm_analysis;
REFRESH MATERIALIZED VIEW analytics.mv_cohort_retention;

\echo '✓ Materialized views refreshed'


-- ============================================================================
-- VERIFICATION
-- ============================================================================

\echo ''
\echo '============================================================================'
\echo '             CUSTOMER ANALYTICS MODULE - COMPLETE                           '
\echo '============================================================================'
\echo ''
\echo '✅ Regular Views (4):'
\echo '   • vw_churn_risk_customers    - At-risk customers prioritized'
\echo '   • vw_customer_demographics   - Age/gender breakdown'
\echo '   • vw_customer_geography      - Location distribution'
\echo '   • vw_new_vs_returning        - Acquisition vs retention'
\echo ''
\echo '✅ Materialized Views (3):'
\echo '   • mv_customer_lifetime_value  - CLV with tiers'
\echo '   • mv_rfm_analysis            - RFM segmentation'
\echo '   • mv_cohort_retention        - Cohort retention rates'
\echo ''
\echo '✅ JSON Functions (6):'
\echo '   • get_top_customers_json()'
\echo '   • get_clv_tier_distribution_json()'
\echo '   • get_rfm_segments_json()'
\echo '   • get_churn_risk_json()'
\echo '   • get_demographics_json()'
\echo '   • get_geography_json()'
\echo ''
\echo '📊 Quick Test:'
\echo '   SELECT clv_tier, COUNT(*), ROUND(SUM(total_revenue)::NUMERIC, 2)'
\echo '   FROM analytics.mv_customer_lifetime_value'
\echo '   GROUP BY clv_tier ORDER BY 3 DESC;'
\echo ''
\echo '➡️  Next: Run 03_product_analytics.sql'
\echo '============================================================================'
\echo ''

-- Show CLV distribution
SELECT 
    clv_tier,
    COUNT(*) as customers,
    ROUND(SUM(total_revenue)::NUMERIC, 2) as total_revenue,
    ROUND(AVG(total_revenue)::NUMERIC, 2) as avg_revenue
FROM analytics.mv_customer_lifetime_value
WHERE total_orders > 0
GROUP BY clv_tier
ORDER BY avg_revenue DESC;





-- ============================================================================
-- FILE: 03_kpi_queries/03_product_analytics.sql
-- PROJECT: RetailMart Enterprise Analytics Platform
-- PURPOSE: Product Analytics Module - Complete product performance tracking
-- AUTHOR: SQL Bootcamp
-- CREATED: 2025
--
-- DESCRIPTION:
--   "80% of your revenue comes from 20% of your products" - Pareto Principle
--   
--   This module answers:
--   - What are our best sellers? (Top Products)
--   - Which products drive most revenue? (ABC Analysis)
--   - How are categories performing? (Category Analysis)
--   - Which brands dominate? (Brand Analysis)
--   - Is inventory moving fast enough? (Inventory Turnover)
--
-- CREATES:
--   • 3 Regular Views
--   • 2 Materialized Views
--   • 5 JSON Export Functions
--
-- EXECUTION ORDER: Run AFTER 02_customer_analytics.sql
-- ============================================================================

\echo ''
\echo '============================================================================'
\echo '              PRODUCT ANALYTICS MODULE - STARTING                           '
\echo '============================================================================'
\echo ''

-- ============================================================================
-- MATERIALIZED VIEW 1: TOP PRODUCTS
-- ============================================================================

\echo '[1/5] Creating materialized view: mv_top_products...'

DROP MATERIALIZED VIEW IF EXISTS analytics.mv_top_products CASCADE;

CREATE MATERIALIZED VIEW analytics.mv_top_products AS
WITH product_sales AS (
    SELECT 
        p.prod_id,
        p.prod_name,
        p.category,
        p.brand,
        p.price as list_price,
        COUNT(DISTINCT oi.order_id) as times_ordered,
        SUM(oi.quantity) as total_units_sold,
        SUM(oi.quantity * oi.unit_price) as gross_revenue,
        SUM(oi.quantity * oi.unit_price * oi.discount / 100) as total_discounts,
        SUM(oi.quantity * oi.unit_price * (1 - oi.discount / 100)) as net_revenue,
        AVG(oi.unit_price) as avg_selling_price,
        AVG(oi.discount) as avg_discount_pct
    FROM products.products p
    JOIN sales.order_items oi ON p.prod_id = oi.prod_id
    JOIN sales.orders o ON oi.order_id = o.order_id AND o.order_status = 'Delivered'
    GROUP BY p.prod_id, p.prod_name, p.category, p.brand, p.price
),
product_reviews AS (
    SELECT prod_id, COUNT(*) as review_count, ROUND(AVG(rating), 2) as avg_rating
    FROM customers.reviews
    GROUP BY prod_id
),
product_inventory AS (
    SELECT prod_id, SUM(stock_qty) as total_stock, COUNT(DISTINCT store_id) as stores_stocking
    FROM products.inventory
    GROUP BY prod_id
)
SELECT 
    ps.prod_id,
    ps.prod_name,
    ps.category,
    ps.brand,
    ROUND(ps.list_price::NUMERIC, 2) as list_price,
    ps.times_ordered,
    ps.total_units_sold,
    ROUND(ps.gross_revenue::NUMERIC, 2) as gross_revenue,
    ROUND(ps.total_discounts::NUMERIC, 2) as total_discounts,
    ROUND(ps.net_revenue::NUMERIC, 2) as net_revenue,
    ROUND(ps.avg_selling_price::NUMERIC, 2) as avg_selling_price,
    ROUND(ps.avg_discount_pct::NUMERIC, 2) as avg_discount_pct,
    COALESCE(pr.review_count, 0) as review_count,
    COALESCE(pr.avg_rating, 0) as avg_rating,
    COALESCE(pi.total_stock, 0) as current_stock,
    COALESCE(pi.stores_stocking, 0) as stores_stocking,
    RANK() OVER (ORDER BY ps.net_revenue DESC) as revenue_rank,
    RANK() OVER (ORDER BY ps.total_units_sold DESC) as units_rank,
    RANK() OVER (PARTITION BY ps.category ORDER BY ps.net_revenue DESC) as category_rank,
    ROUND((ps.net_revenue / SUM(ps.net_revenue) OVER () * 100)::NUMERIC, 4) as pct_of_total_revenue
FROM product_sales ps
LEFT JOIN product_reviews pr ON ps.prod_id = pr.prod_id
LEFT JOIN product_inventory pi ON ps.prod_id = pi.prod_id;

CREATE INDEX IF NOT EXISTS idx_top_products_category ON analytics.mv_top_products(category);
CREATE INDEX IF NOT EXISTS idx_top_products_rank ON analytics.mv_top_products(revenue_rank);

\echo '      ✓ Materialized view created: mv_top_products'


-- ============================================================================
-- MATERIALIZED VIEW 2: ABC ANALYSIS (Pareto)
-- ============================================================================

\echo '[2/5] Creating materialized view: mv_abc_analysis...'

DROP MATERIALIZED VIEW IF EXISTS analytics.mv_abc_analysis CASCADE;

CREATE MATERIALIZED VIEW analytics.mv_abc_analysis AS
WITH product_revenue AS (
    SELECT 
        p.prod_id,
        p.prod_name,
        p.category,
        p.brand,
        SUM(oi.quantity * oi.unit_price * (1 - oi.discount / 100)) as net_revenue
    FROM products.products p
    JOIN sales.order_items oi ON p.prod_id = oi.prod_id
    JOIN sales.orders o ON oi.order_id = o.order_id AND o.order_status = 'Delivered'
    GROUP BY p.prod_id, p.prod_name, p.category, p.brand
),
with_cumulative AS (
    SELECT 
        *,
        SUM(net_revenue) OVER (ORDER BY net_revenue DESC) as cumulative_revenue,
        SUM(net_revenue) OVER () as total_revenue
    FROM product_revenue
)
SELECT 
    prod_id,
    prod_name,
    category,
    brand,
    ROUND(net_revenue::NUMERIC, 2) as net_revenue,
    ROUND((net_revenue / total_revenue * 100)::NUMERIC, 4) as pct_of_revenue,
    ROUND((cumulative_revenue / total_revenue * 100)::NUMERIC, 2) as cumulative_pct,
    CASE 
        WHEN cumulative_revenue / total_revenue <= 0.80 THEN 'A'
        WHEN cumulative_revenue / total_revenue <= 0.95 THEN 'B'
        ELSE 'C'
    END as abc_classification,
    ROW_NUMBER() OVER (ORDER BY net_revenue DESC) as revenue_rank
FROM with_cumulative
ORDER BY net_revenue DESC;

\echo '      ✓ Materialized view created: mv_abc_analysis'


-- ============================================================================
-- VIEW 1: CATEGORY PERFORMANCE
-- ============================================================================

\echo '[3/5] Creating view: vw_category_performance...'

CREATE OR REPLACE VIEW analytics.vw_category_performance AS
WITH category_stats AS (
    SELECT 
        p.category,
        COUNT(DISTINCT p.prod_id) as product_count,
        COUNT(DISTINCT oi.order_id) as order_count,
        SUM(oi.quantity) as units_sold,
        SUM(oi.quantity * oi.unit_price * (1 - oi.discount / 100)) as net_revenue,
        AVG(oi.unit_price) as avg_price,
        AVG(oi.discount) as avg_discount_pct
    FROM products.products p
    JOIN sales.order_items oi ON p.prod_id = oi.prod_id
    JOIN sales.orders o ON oi.order_id = o.order_id AND o.order_status = 'Delivered'
    GROUP BY p.category
),
category_reviews AS (
    SELECT p.category, COUNT(*) as total_reviews, AVG(r.rating) as avg_rating
    FROM customers.reviews r
    JOIN products.products p ON r.prod_id = p.prod_id
    GROUP BY p.category
)
SELECT 
    cs.category,
    cs.product_count,
    cs.order_count,
    cs.units_sold,
    ROUND(cs.net_revenue::NUMERIC, 2) as net_revenue,
    ROUND(cs.avg_price::NUMERIC, 2) as avg_price,
    ROUND(cs.avg_discount_pct::NUMERIC, 2) as avg_discount_pct,
    COALESCE(cr.total_reviews, 0) as total_reviews,
    ROUND(COALESCE(cr.avg_rating, 0)::NUMERIC, 2) as avg_rating,
    ROUND((cs.net_revenue / SUM(cs.net_revenue) OVER () * 100)::NUMERIC, 2) as market_share_pct,
    RANK() OVER (ORDER BY cs.net_revenue DESC) as revenue_rank
FROM category_stats cs
LEFT JOIN category_reviews cr ON cs.category = cr.category
ORDER BY net_revenue DESC;

\echo '      ✓ View created: vw_category_performance'


-- ============================================================================
-- VIEW 2: BRAND PERFORMANCE
-- ============================================================================

\echo '[4/5] Creating view: vw_brand_performance...'

CREATE OR REPLACE VIEW analytics.vw_brand_performance AS
SELECT 
    brand,
    category,
    COUNT(DISTINCT prod_id) as product_count,
    SUM(total_units_sold) as total_units_sold,
    ROUND(SUM(net_revenue)::NUMERIC, 2) as net_revenue,
    ROUND(AVG(avg_rating)::NUMERIC, 2) as avg_rating,
    SUM(review_count) as review_count,
    ROUND((SUM(net_revenue) / SUM(SUM(net_revenue)) OVER (PARTITION BY category) * 100)::NUMERIC, 2) as category_market_share_pct,
    RANK() OVER (PARTITION BY category ORDER BY SUM(net_revenue) DESC) as category_rank
FROM analytics.mv_top_products
GROUP BY brand, category
ORDER BY net_revenue DESC;

\echo '      ✓ View created: vw_brand_performance'


-- ============================================================================
-- VIEW 3: INVENTORY TURNOVER
-- ============================================================================

\echo '[5/5] Creating view: vw_inventory_turnover...'

CREATE OR REPLACE VIEW analytics.vw_inventory_turnover AS
WITH product_velocity AS (
    SELECT 
        p.prod_id,
        p.prod_name,
        p.category,
        i.stock_qty as current_stock,
        COALESCE(SUM(oi.quantity), 0) as units_sold_30d
    FROM products.products p
    LEFT JOIN products.inventory i ON p.prod_id = i.prod_id
    LEFT JOIN sales.order_items oi ON p.prod_id = oi.prod_id
    LEFT JOIN sales.orders o ON oi.order_id = o.order_id 
        AND o.order_status = 'Delivered'
        AND o.order_date >= (SELECT MAX(order_date) - INTERVAL '30 days' FROM sales.orders)
    GROUP BY p.prod_id, p.prod_name, p.category, i.stock_qty
)
SELECT 
    prod_id,
    prod_name,
    category,
    COALESCE(current_stock, 0) as current_stock,
    units_sold_30d,
    ROUND(units_sold_30d / 30.0, 2) as daily_velocity,
    CASE 
        WHEN units_sold_30d > 0 THEN ROUND(COALESCE(current_stock, 0) / (units_sold_30d / 30.0), 0)
        ELSE 9999
    END as days_of_inventory,
    CASE 
        WHEN COALESCE(current_stock, 0) = 0 THEN 'Out of Stock'
        WHEN units_sold_30d = 0 THEN 'Dead Stock'
        WHEN COALESCE(current_stock, 0) / NULLIF(units_sold_30d / 30.0, 0) < 7 THEN 'Low Stock'
        WHEN COALESCE(current_stock, 0) / NULLIF(units_sold_30d / 30.0, 0) > 90 THEN 'Overstocked'
        ELSE 'Normal'
    END as stock_status
FROM product_velocity
ORDER BY days_of_inventory;

\echo '      ✓ View created: vw_inventory_turnover'


-- ============================================================================
-- JSON EXPORT FUNCTIONS
-- ============================================================================

\echo ''
\echo 'Creating JSON export functions...'

CREATE OR REPLACE FUNCTION analytics.get_top_products_json()
RETURNS JSON AS $$
BEGIN
    RETURN (
        SELECT json_agg(
            json_build_object(
                'prodId', prod_id, 'productName', prod_name, 'category', category, 'brand', brand,
                'revenue', net_revenue, 'unitsSold', total_units_sold, 'avgRating', avg_rating,
                'currentStock', current_stock, 'revenueRank', revenue_rank, 'categoryRank', category_rank
            ) ORDER BY revenue_rank
        )
        FROM analytics.mv_top_products
        LIMIT 20
    );
END;
$$ LANGUAGE plpgsql STABLE;

CREATE OR REPLACE FUNCTION analytics.get_category_performance_json()
RETURNS JSON AS $$
BEGIN
    RETURN (
        SELECT json_agg(
            json_build_object(
                'category', category, 'productCount', product_count, 'revenue', net_revenue,
                'unitsSold', units_sold, 'avgPrice', avg_price, 'avgRating', avg_rating,
                'marketShare', market_share_pct, 'rank', revenue_rank
            ) ORDER BY revenue_rank
        )
        FROM analytics.vw_category_performance
    );
END;
$$ LANGUAGE plpgsql STABLE;

CREATE OR REPLACE FUNCTION analytics.get_brand_performance_json()
RETURNS JSON AS $$
BEGIN
    RETURN (
        SELECT json_agg(
            json_build_object(
                'brand', brand, 'category', category, 'productCount', product_count,
                'revenue', net_revenue, 'unitsSold', total_units_sold, 'avgRating', avg_rating,
                'categoryMarketShare', category_market_share_pct, 'categoryRank', category_rank
            ) ORDER BY net_revenue DESC
        )
        FROM analytics.vw_brand_performance
        LIMIT 20
    );
END;
$$ LANGUAGE plpgsql STABLE;

CREATE OR REPLACE FUNCTION analytics.get_abc_analysis_json()
RETURNS JSON AS $$
BEGIN
    RETURN (
        SELECT json_build_object(
            'summary', (
                SELECT json_agg(json_build_object(
                    'class', abc_classification, 'productCount', cnt, 'totalRevenue', revenue, 'pctOfRevenue', pct
                ))
                FROM (
                    SELECT abc_classification, COUNT(*) as cnt, 
                           ROUND(SUM(net_revenue)::NUMERIC, 2) as revenue,
                           ROUND((SUM(net_revenue) / SUM(SUM(net_revenue)) OVER () * 100)::NUMERIC, 2) as pct
                    FROM analytics.mv_abc_analysis
                    GROUP BY abc_classification
                ) s
            ),
            'topAProducts', (
                SELECT json_agg(json_build_object('productName', prod_name, 'revenue', net_revenue, 'pct', pct_of_revenue))
                FROM analytics.mv_abc_analysis WHERE abc_classification = 'A' LIMIT 20
            )
        )
    );
END;
$$ LANGUAGE plpgsql STABLE;

CREATE OR REPLACE FUNCTION analytics.get_inventory_status_json()
RETURNS JSON AS $$
BEGIN
    RETURN (
        SELECT json_agg(json_build_object(
            'status', stock_status, 'productCount', cnt, 'pctOfProducts', pct
        ))
        FROM (
            SELECT stock_status, COUNT(*) as cnt,
                   ROUND((COUNT(*)::NUMERIC / SUM(COUNT(*)) OVER () * 100), 2) as pct
            FROM analytics.vw_inventory_turnover
            GROUP BY stock_status
        ) s
    );
END;
$$ LANGUAGE plpgsql STABLE;

\echo '      ✓ JSON functions created (5 functions)'


-- Refresh MVs
REFRESH MATERIALIZED VIEW analytics.mv_top_products;
REFRESH MATERIALIZED VIEW analytics.mv_abc_analysis;

\echo ''
\echo '============================================================================'
\echo '              PRODUCT ANALYTICS MODULE - COMPLETE                           '
\echo '============================================================================'
\echo ''



-- ============================================================================
-- FILE: 03_kpi_queries/04_store_analytics.sql
-- PROJECT: RetailMart Enterprise Analytics Platform
-- PURPOSE: Store Analytics Module - Store performance and profitability tracking
-- AUTHOR: SQL Bootcamp
-- CREATED: 2025
--
-- DESCRIPTION:
--   At DMart, every month leadership asks: "Which stores are stars? Which need help?"
--   This module provides store-level P&L, regional comparisons, and efficiency metrics.
--
-- CREATES:
--   • 3 Regular Views
--   • 1 Materialized View
--   • 4 JSON Export Functions
-- ============================================================================

\echo ''
\echo '============================================================================'
\echo '               STORE ANALYTICS MODULE - STARTING                            '
\echo '============================================================================'
\echo ''

-- ============================================================================
-- MATERIALIZED VIEW: STORE PERFORMANCE
-- ============================================================================

\echo '[1/4] Creating materialized view: mv_store_performance...'

DROP MATERIALIZED VIEW IF EXISTS analytics.mv_store_performance CASCADE;

CREATE MATERIALIZED VIEW analytics.mv_store_performance AS
WITH store_sales AS (
    SELECT 
        s.store_id, s.store_name, s.city, s.state, s.region,
        COUNT(DISTINCT o.order_id) as total_orders,
        SUM(o.total_amount) as total_revenue,
        AVG(o.total_amount) as avg_order_value,
        COUNT(DISTINCT o.cust_id) as unique_customers
    FROM stores.stores s
    LEFT JOIN sales.orders o ON s.store_id = o.store_id AND o.order_status = 'Delivered'
    GROUP BY s.store_id, s.store_name, s.city, s.state, s.region
),
store_expenses AS (
    SELECT store_id, SUM(amount) as total_expenses
    FROM stores.expenses
    GROUP BY store_id
),
store_employees AS (
    SELECT store_id, COUNT(*) as employee_count, SUM(salary) as total_payroll
    FROM stores.employees
    GROUP BY store_id
)
SELECT 
    ss.store_id, ss.store_name, ss.city, ss.state, ss.region,
    ss.total_orders, 
    ROUND(COALESCE(ss.total_revenue, 0)::NUMERIC, 2) as total_revenue,
    ROUND(COALESCE(ss.avg_order_value, 0)::NUMERIC, 2) as avg_order_value,
    ss.unique_customers,
    ROUND(COALESCE(se.total_expenses, 0)::NUMERIC, 2) as total_expenses,
    ROUND((COALESCE(ss.total_revenue, 0) - COALESCE(se.total_expenses, 0))::NUMERIC, 2) as net_profit,
    ROUND(((COALESCE(ss.total_revenue, 0) - COALESCE(se.total_expenses, 0)) / 
           NULLIF(ss.total_revenue, 0) * 100)::NUMERIC, 2) as profit_margin_pct,
    COALESCE(emp.employee_count, 0) as employee_count,
    ROUND(COALESCE(emp.total_payroll, 0)::NUMERIC, 2) as total_payroll,
    ROUND((COALESCE(ss.total_revenue, 0) / NULLIF(emp.employee_count, 0))::NUMERIC, 2) as revenue_per_employee,
    RANK() OVER (ORDER BY ss.total_revenue DESC NULLS LAST) as revenue_rank,
    RANK() OVER (ORDER BY (COALESCE(ss.total_revenue, 0) - COALESCE(se.total_expenses, 0)) DESC) as profit_rank,
    CASE 
        WHEN PERCENT_RANK() OVER (ORDER BY ss.total_revenue NULLS FIRST) >= 0.8 THEN 'Star'
        WHEN PERCENT_RANK() OVER (ORDER BY ss.total_revenue NULLS FIRST) >= 0.5 THEN 'Average'
        WHEN PERCENT_RANK() OVER (ORDER BY ss.total_revenue NULLS FIRST) >= 0.2 THEN 'Improving'
        ELSE 'Needs Attention'
    END as performance_tier
FROM store_sales ss
LEFT JOIN store_expenses se ON ss.store_id = se.store_id
LEFT JOIN store_employees emp ON ss.store_id = emp.store_id;

CREATE INDEX IF NOT EXISTS idx_store_perf_region ON analytics.mv_store_performance(region);

\echo '      ✓ Materialized view created: mv_store_performance'


-- ============================================================================
-- VIEW 1: REGIONAL PERFORMANCE
-- ============================================================================

\echo '[2/4] Creating view: vw_regional_performance...'

CREATE OR REPLACE VIEW analytics.vw_regional_performance AS
SELECT 
    region,
    COUNT(DISTINCT store_id) as store_count,
    SUM(total_orders) as total_orders,
    ROUND(SUM(total_revenue)::NUMERIC, 2) as total_revenue,
    ROUND(AVG(avg_order_value)::NUMERIC, 2) as avg_order_value,
    SUM(unique_customers) as total_customers,
    ROUND(SUM(total_expenses)::NUMERIC, 2) as total_expenses,
    ROUND(SUM(net_profit)::NUMERIC, 2) as total_profit,
    ROUND(AVG(profit_margin_pct)::NUMERIC, 2) as avg_profit_margin,
    SUM(employee_count) as total_employees,
    ROUND((SUM(total_revenue) / NULLIF(SUM(employee_count), 0))::NUMERIC, 2) as revenue_per_employee,
    ROUND((SUM(total_revenue) / COUNT(DISTINCT store_id))::NUMERIC, 2) as avg_revenue_per_store
FROM analytics.mv_store_performance
GROUP BY region
ORDER BY total_revenue DESC;

\echo '      ✓ View created: vw_regional_performance'


-- ============================================================================
-- VIEW 2: STORE INVENTORY STATUS
-- ============================================================================

\echo '[3/4] Creating view: vw_store_inventory_status...'

CREATE OR REPLACE VIEW analytics.vw_store_inventory_status AS
SELECT 
    s.store_id, s.store_name, s.city, s.region,
    COUNT(DISTINCT i.prod_id) as products_stocked,
    SUM(i.stock_qty) as total_units,
    SUM(i.stock_qty * p.price) as inventory_value,
    COUNT(*) FILTER (WHERE i.stock_qty = 0) as out_of_stock_count,
    COUNT(*) FILTER (WHERE i.stock_qty < 10 AND i.stock_qty > 0) as low_stock_count,
    CASE 
        WHEN COUNT(*) FILTER (WHERE i.stock_qty = 0) > 20 THEN 'Critical'
        WHEN COUNT(*) FILTER (WHERE i.stock_qty < 10) > 50 THEN 'Warning'
        ELSE 'Healthy'
    END as inventory_health
FROM stores.stores s
LEFT JOIN products.inventory i ON s.store_id = i.store_id
LEFT JOIN products.products p ON i.prod_id = p.prod_id
GROUP BY s.store_id, s.store_name, s.city, s.region
ORDER BY inventory_value DESC;

\echo '      ✓ View created: vw_store_inventory_status'


-- ============================================================================
-- VIEW 3: EMPLOYEE BY STORE
-- ============================================================================

\echo '[4/4] Creating view: vw_employee_by_store...'

CREATE OR REPLACE VIEW analytics.vw_employee_by_store AS
SELECT 
    s.store_id, s.store_name, s.city, s.region,
    COUNT(e.emp_id) as employee_count,
    ROUND(SUM(e.salary)::NUMERIC, 2) as total_payroll,
    ROUND(AVG(e.salary)::NUMERIC, 2) as avg_salary,
    COUNT(DISTINCT e.role) as unique_roles,
    STRING_AGG(DISTINCT e.role, ', ') as roles
FROM stores.stores s
LEFT JOIN stores.employees e ON s.store_id = e.store_id
GROUP BY s.store_id, s.store_name, s.city, s.region
ORDER BY total_payroll DESC;

\echo '      ✓ View created: vw_employee_by_store'


-- ============================================================================
-- JSON EXPORT FUNCTIONS
-- ============================================================================

\echo ''
\echo 'Creating JSON export functions...'

CREATE OR REPLACE FUNCTION analytics.get_top_stores_json()
RETURNS JSON AS $$
BEGIN
    RETURN (
        SELECT json_agg(json_build_object(
            'storeId', store_id, 'storeName', store_name, 'city', city, 'region', region,
            'revenue', total_revenue, 'profit', net_profit, 'profitMargin', profit_margin_pct,
            'orders', total_orders, 'employees', employee_count, 'performanceTier', performance_tier,
            'revenueRank', revenue_rank
        ) ORDER BY revenue_rank)
        FROM analytics.mv_store_performance
        LIMIT 20
    );
END;
$$ LANGUAGE plpgsql STABLE;

CREATE OR REPLACE FUNCTION analytics.get_regional_performance_json()
RETURNS JSON AS $$
BEGIN
    RETURN (
        SELECT json_agg(json_build_object(
            'region', region, 'storeCount', store_count, 'revenue', total_revenue,
            'profit', total_profit, 'avgProfitMargin', avg_profit_margin,
            'employees', total_employees, 'revenuePerEmployee', revenue_per_employee
        ) ORDER BY total_revenue DESC)
        FROM analytics.vw_regional_performance
    );
END;
$$ LANGUAGE plpgsql STABLE;

CREATE OR REPLACE FUNCTION analytics.get_store_inventory_json()
RETURNS JSON AS $$
BEGIN
    RETURN (
        SELECT json_agg(json_build_object(
            'storeName', store_name, 'city', city, 'region', region,
            'productsStocked', products_stocked, 'inventoryValue', ROUND(inventory_value::NUMERIC, 2),
            'outOfStock', out_of_stock_count, 'lowStock', low_stock_count, 'health', inventory_health
        ) ORDER BY inventory_value DESC)
        FROM analytics.vw_store_inventory_status
        LIMIT 20
    );
END;
$$ LANGUAGE plpgsql STABLE;

CREATE OR REPLACE FUNCTION analytics.get_employee_distribution_json()
RETURNS JSON AS $$
BEGIN
    RETURN (
        SELECT json_agg(json_build_object(
            'storeName', store_name, 'city', city, 'region', region,
            'employees', employee_count, 'totalPayroll', total_payroll, 'avgSalary', avg_salary
        ) ORDER BY total_payroll DESC)
        FROM analytics.vw_employee_by_store
        LIMIT 20
    );
END;
$$ LANGUAGE plpgsql STABLE;

\echo '      ✓ JSON functions created (4 functions)'

REFRESH MATERIALIZED VIEW analytics.mv_store_performance;

\echo ''
\echo '============================================================================'
\echo '               STORE ANALYTICS MODULE - COMPLETE                            '
\echo '============================================================================'
\echo ''


-- ============================================================================
-- FILE: 03_kpi_queries/05_operations_analytics.sql
-- PROJECT: RetailMart Enterprise Analytics Platform
-- PURPOSE: Operations Analytics - Delivery, Returns, Payments tracking
-- AUTHOR: SQL Bootcamp
-- CREATED: 2025
--
-- DESCRIPTION:
--   Operations is where the rubber meets the road. At Amazon, they obsess over:
--   - On-time delivery rate (SLA compliance)
--   - Return rates and reasons
--   - Payment success rates
--   
--   This module tracks operational health and identifies bottlenecks.
--
-- CREATES:
--   • 5 Regular Views
--   • 1 Materialized View
--   • 5 JSON Export Functions
-- ============================================================================

\echo ''
\echo '============================================================================'
\echo '             OPERATIONS ANALYTICS MODULE - STARTING                         '
\echo '============================================================================'
\echo ''

-- ============================================================================
-- VIEW 1: DELIVERY PERFORMANCE
-- ============================================================================

\echo '[1/6] Creating view: vw_delivery_performance...'

CREATE OR REPLACE VIEW analytics.vw_delivery_performance AS
WITH delivery_metrics AS (
    SELECT 
        DATE_TRUNC('month', s.shipped_date)::DATE as ship_month,
        COUNT(*) as total_shipments,
        COUNT(*) FILTER (WHERE s.status = 'Delivered') as delivered_count,
        COUNT(*) FILTER (WHERE s.status = 'Shipped') as in_transit_count,
        COUNT(*) FILTER (WHERE s.status = 'Pending') as pending_count,
        AVG(s.delivered_date - s.shipped_date) as avg_delivery_days,
        COUNT(*) FILTER (WHERE (s.delivered_date - s.shipped_date) <= 3) as on_time_deliveries,
        COUNT(*) FILTER (WHERE (s.delivered_date - s.shipped_date) > 7) as delayed_deliveries
    FROM sales.shipments s
    WHERE s.shipped_date IS NOT NULL
    GROUP BY DATE_TRUNC('month', s.shipped_date)
)
SELECT 
    ship_month,
    TO_CHAR(ship_month, 'Mon YYYY') as month_name,
    total_shipments,
    delivered_count,
    in_transit_count,
    pending_count,
    ROUND(avg_delivery_days::NUMERIC, 1) as avg_delivery_days,
    on_time_deliveries,
    delayed_deliveries,
    ROUND((on_time_deliveries::NUMERIC / NULLIF(delivered_count, 0) * 100), 2) as on_time_pct,
    ROUND((delayed_deliveries::NUMERIC / NULLIF(delivered_count, 0) * 100), 2) as delayed_pct,
    CASE 
        WHEN (on_time_deliveries::NUMERIC / NULLIF(delivered_count, 0) * 100) >= 95 THEN 'Excellent'
        WHEN (on_time_deliveries::NUMERIC / NULLIF(delivered_count, 0) * 100) >= 85 THEN 'Good'
        WHEN (on_time_deliveries::NUMERIC / NULLIF(delivered_count, 0) * 100) >= 70 THEN 'Needs Improvement'
        ELSE 'Critical'
    END as sla_status
FROM delivery_metrics
ORDER BY ship_month DESC;

\echo '      ✓ View created: vw_delivery_performance'


-- ============================================================================
-- VIEW 2: COURIER COMPARISON
-- ============================================================================

\echo '[2/6] Creating view: vw_courier_comparison...'

CREATE OR REPLACE VIEW analytics.vw_courier_comparison AS
SELECT 
    courier_name,
    COUNT(*) as total_shipments,
    COUNT(*) FILTER (WHERE status = 'Delivered') as delivered,
    COUNT(*) FILTER (WHERE status = 'Shipped') as in_transit,
    ROUND(AVG(delivered_date - shipped_date)::NUMERIC, 1) as avg_delivery_days,
    COUNT(*) FILTER (WHERE (delivered_date - shipped_date) <= 3) as on_time,
    ROUND((COUNT(*) FILTER (WHERE (delivered_date - shipped_date) <= 3)::NUMERIC / 
           NULLIF(COUNT(*) FILTER (WHERE status = 'Delivered'), 0) * 100), 2) as on_time_pct,
    RANK() OVER (ORDER BY AVG(delivered_date - shipped_date) NULLS LAST) as speed_rank,
    RANK() OVER (ORDER BY COUNT(*) FILTER (WHERE (delivered_date - shipped_date) <= 3)::NUMERIC / 
           NULLIF(COUNT(*) FILTER (WHERE status = 'Delivered'), 0) DESC NULLS LAST) as reliability_rank
FROM sales.shipments
WHERE shipped_date IS NOT NULL
GROUP BY courier_name
ORDER BY on_time_pct DESC NULLS LAST;

\echo '      ✓ View created: vw_courier_comparison'


-- ============================================================================
-- VIEW 3: RETURN ANALYSIS
-- ============================================================================

\echo '[3/6] Creating view: vw_return_analysis...'

CREATE OR REPLACE VIEW analytics.vw_return_analysis AS
WITH return_stats AS (
    SELECT 
        p.category,
        COUNT(*) as return_count,
        SUM(r.refund_amount) as total_refunds,
        r.reason
    FROM sales.returns r
    JOIN products.products p ON r.prod_id = p.prod_id
    GROUP BY p.category, r.reason
),
category_orders AS (
    SELECT 
        p.category,
        COUNT(DISTINCT oi.order_id) as total_orders,
        SUM(oi.quantity * oi.unit_price) as total_revenue
    FROM sales.order_items oi
    JOIN products.products p ON oi.prod_id = p.prod_id
    JOIN sales.orders o ON oi.order_id = o.order_id AND o.order_status = 'Delivered'
    GROUP BY p.category
)
SELECT 
    rs.category,
    rs.reason,
    rs.return_count,
    ROUND(rs.total_refunds::NUMERIC, 2) as total_refunds,
    co.total_orders,
    ROUND((rs.return_count::NUMERIC / NULLIF(co.total_orders, 0) * 100), 2) as return_rate_pct,
    ROUND((rs.total_refunds / NULLIF(co.total_revenue, 0) * 100), 2) as refund_rate_pct
FROM return_stats rs
JOIN category_orders co ON rs.category = co.category
ORDER BY return_count DESC;

\echo '      ✓ View created: vw_return_analysis'


-- ============================================================================
-- VIEW 4: PAYMENT SUCCESS RATE
-- ============================================================================

\echo '[4/6] Creating view: vw_payment_success_rate...'

CREATE OR REPLACE VIEW analytics.vw_payment_success_rate AS
WITH payment_stats AS (
    SELECT 
        payment_mode,
        DATE_TRUNC('month', payment_date)::DATE as payment_month,
        COUNT(*) as total_transactions,
        SUM(amount) as total_amount,
        AVG(amount) as avg_amount
    FROM sales.payments
    GROUP BY payment_mode, DATE_TRUNC('month', payment_date)
)
SELECT 
    payment_month,
    TO_CHAR(payment_month, 'Mon YYYY') as month_name,
    payment_mode,
    total_transactions,
    ROUND(total_amount::NUMERIC, 2) as total_amount,
    ROUND(avg_amount::NUMERIC, 2) as avg_amount,
    ROUND((total_amount / SUM(total_amount) OVER (PARTITION BY payment_month) * 100)::NUMERIC, 2) as pct_of_monthly_revenue,
    LAG(total_amount) OVER (PARTITION BY payment_mode ORDER BY payment_month) as prev_month_amount,
    ROUND(((total_amount - LAG(total_amount) OVER (PARTITION BY payment_mode ORDER BY payment_month)) /
           NULLIF(LAG(total_amount) OVER (PARTITION BY payment_mode ORDER BY payment_month), 0) * 100)::NUMERIC, 2) as mom_growth_pct
FROM payment_stats
ORDER BY payment_month DESC, total_amount DESC;

\echo '      ✓ View created: vw_payment_success_rate'


-- ============================================================================
-- VIEW 5: PENDING SHIPMENTS (Actionable)
-- ============================================================================

\echo '[5/6] Creating view: vw_pending_shipments...'

CREATE OR REPLACE VIEW analytics.vw_pending_shipments AS
SELECT 
    o.order_id,
    o.order_date,
    (SELECT MAX(order_date) FROM sales.orders) - o.order_date as days_since_order,
    c.full_name as customer_name,
    c.city as customer_city,
    s.store_name,
    o.total_amount,
    sh.status as shipment_status,
    sh.courier_name,
    sh.shipped_date,
    CASE 
        WHEN (SELECT MAX(order_date) FROM sales.orders) - o.order_date > 7 THEN 'Critical'
        WHEN (SELECT MAX(order_date) FROM sales.orders) - o.order_date > 3 THEN 'Urgent'
        ELSE 'Normal'
    END as priority
FROM sales.orders o
LEFT JOIN sales.shipments sh ON o.order_id = sh.order_id
JOIN customers.customers c ON o.cust_id = c.cust_id
JOIN stores.stores s ON o.store_id = s.store_id
WHERE o.order_status != 'Delivered'
AND (sh.status IS NULL OR sh.status != 'Delivered')
ORDER BY days_since_order DESC;

\echo '      ✓ View created: vw_pending_shipments'


-- ============================================================================
-- MATERIALIZED VIEW: OPERATIONS SUMMARY
-- ============================================================================

\echo '[6/6] Creating materialized view: mv_operations_summary...'

DROP MATERIALIZED VIEW IF EXISTS analytics.mv_operations_summary CASCADE;

CREATE MATERIALIZED VIEW analytics.mv_operations_summary AS
WITH delivery_stats AS (
    SELECT 
        COUNT(*) as total_shipments,
        COUNT(*) FILTER (WHERE status = 'Delivered') as delivered,
        ROUND(AVG(delivered_date - shipped_date)::NUMERIC, 1) as avg_delivery_days,
        ROUND((COUNT(*) FILTER (WHERE (delivered_date - shipped_date) <= 3)::NUMERIC / 
               NULLIF(COUNT(*) FILTER (WHERE status = 'Delivered'), 0) * 100), 2) as on_time_pct
    FROM sales.shipments WHERE shipped_date IS NOT NULL
),
return_stats AS (
    SELECT 
        COUNT(*) as total_returns,
        ROUND(SUM(refund_amount)::NUMERIC, 2) as total_refunds,
        ROUND(AVG(refund_amount)::NUMERIC, 2) as avg_refund
    FROM sales.returns
),
payment_stats AS (
    SELECT 
        COUNT(*) as total_payments,
        ROUND(SUM(amount)::NUMERIC, 2) as total_payment_amount,
        COUNT(DISTINCT payment_mode) as payment_modes_used
    FROM sales.payments
),
order_stats AS (
    SELECT 
        COUNT(*) as total_orders,
        COUNT(*) FILTER (WHERE order_status = 'Delivered') as delivered_orders,
        COUNT(*) FILTER (WHERE order_status = 'Pending') as pending_orders,
        COUNT(*) FILTER (WHERE order_status = 'Processing') as processing_orders
    FROM sales.orders
)
SELECT 
    (SELECT MAX(order_date) FROM sales.orders) as reference_date,
    d.total_shipments, d.delivered as shipments_delivered, d.avg_delivery_days, d.on_time_pct as delivery_sla_pct,
    r.total_returns, r.total_refunds, r.avg_refund,
    ROUND((r.total_returns::NUMERIC / NULLIF(o.delivered_orders, 0) * 100), 2) as return_rate_pct,
    p.total_payments, p.total_payment_amount, p.payment_modes_used,
    o.total_orders, o.delivered_orders, o.pending_orders, o.processing_orders
FROM delivery_stats d
CROSS JOIN return_stats r
CROSS JOIN payment_stats p
CROSS JOIN order_stats o;

\echo '      ✓ Materialized view created: mv_operations_summary'


-- ============================================================================
-- JSON EXPORT FUNCTIONS
-- ============================================================================

\echo ''
\echo 'Creating JSON export functions...'

CREATE OR REPLACE FUNCTION analytics.get_operations_summary_json()
RETURNS JSON AS $$
BEGIN
    RETURN (SELECT row_to_json(t) FROM analytics.mv_operations_summary t);
END;
$$ LANGUAGE plpgsql STABLE;

CREATE OR REPLACE FUNCTION analytics.get_delivery_performance_json()
RETURNS JSON AS $$
BEGIN
    RETURN (
        SELECT json_agg(json_build_object(
            'month', month_name, 
            'shipments', total_shipments, 
            'delivered', delivered_count,
            'avgDeliveryDays', avg_delivery_days,
            'onTimePct', on_time_pct, 
            'slaStatus', sla_status
        ) ORDER BY ship_month DESC)
        FROM analytics.vw_delivery_performance
        LIMIT 12
    );
END;
$$ LANGUAGE plpgsql STABLE;

CREATE OR REPLACE FUNCTION analytics.get_courier_comparison_json()
RETURNS JSON AS $$
BEGIN
    RETURN (
        SELECT json_agg(json_build_object(
            'courier', courier_name, 
            'shipments', total_shipments, 
            'avgDays', avg_delivery_days,
            'onTimePct', on_time_pct, 
            'speedRank', speed_rank, 
            'reliabilityRank', reliability_rank,
            'performanceScore', ROUND((COALESCE(on_time_pct, 0) * 0.7 + (100 - LEAST(avg_delivery_days * 10, 100)) * 0.3)::NUMERIC, 1)
        ) ORDER BY on_time_pct DESC NULLS LAST)
        FROM analytics.vw_courier_comparison
    );
END;
$$ LANGUAGE plpgsql STABLE;

CREATE OR REPLACE FUNCTION analytics.get_return_analysis_json()
RETURNS JSON AS $$
BEGIN
    RETURN json_build_object(
        'byCategory', (
            SELECT json_agg(json_build_object(
                'category', category,
                'returnCount', return_count,
                'totalRefunds', total_refunds,
                'returnRate', return_rate_pct
            ) ORDER BY return_count DESC)
            FROM (
                SELECT 
                    category,
                    SUM(return_count) as return_count,
                    SUM(total_refunds) as total_refunds,
                    ROUND(AVG(return_rate_pct)::NUMERIC, 2) as return_rate_pct
                FROM analytics.vw_return_analysis
                GROUP BY category
            ) cat_summary
        ),
        'byReason', (
            SELECT json_agg(json_build_object(
                'reason', reason,
                'count', return_count
            ) ORDER BY return_count DESC)
            FROM (
                SELECT 
                    reason,
                    SUM(return_count) as return_count
                FROM analytics.vw_return_analysis
                GROUP BY reason
            ) reason_summary
        )
    );
END;
$$ LANGUAGE plpgsql STABLE;

CREATE OR REPLACE FUNCTION analytics.get_pending_shipments_json()
RETURNS JSON AS $$
BEGIN
    RETURN (
        SELECT json_agg(json_build_object(
            'orderId', order_id, 'orderDate', order_date, 'daysSince', days_since_order,
            'customer', customer_name, 'city', customer_city, 'amount', total_amount,
            'status', shipment_status, 'priority', priority
        ) ORDER BY days_since_order DESC)
        FROM analytics.vw_pending_shipments
        LIMIT 50
    );
END;
$$ LANGUAGE plpgsql STABLE;

\echo '      ✓ JSON functions created (5 functions)'

REFRESH MATERIALIZED VIEW analytics.mv_operations_summary;

\echo ''
\echo '============================================================================'
\echo '             OPERATIONS ANALYTICS MODULE - COMPLETE                         '
\echo '============================================================================'
\echo ''



-- ============================================================================
-- FILE: 03_kpi_queries/06_marketing_analytics.sql
-- PROJECT: RetailMart Enterprise Analytics Platform
-- PURPOSE: Marketing Analytics - Campaign ROI, Promotion effectiveness
-- AUTHOR: SQL Bootcamp
-- CREATED: 2025
-- ============================================================================

\echo ''
\echo '============================================================================'
\echo '             MARKETING ANALYTICS MODULE - STARTING                          '
\echo '============================================================================'
\echo ''

\echo '[1/5] Creating view: vw_campaign_performance...'

CREATE OR REPLACE VIEW analytics.vw_campaign_performance AS
WITH campaign_spend AS (
    SELECT 
        c.campaign_id, c.campaign_name, c.start_date, c.end_date, c.budget,
        COALESCE(SUM(a.amount), 0) as actual_spend,
        COALESCE(SUM(a.clicks), 0) as total_clicks,
        COALESCE(SUM(a.conversions), 0) as total_conversions
    FROM marketing.campaigns c
    LEFT JOIN marketing.ads_spend a ON c.campaign_id = a.campaign_id
    GROUP BY c.campaign_id, c.campaign_name, c.start_date, c.end_date, c.budget
),
campaign_revenue AS (
    SELECT c.campaign_id,
        COUNT(DISTINCT o.order_id) as orders_during_campaign,
        COALESCE(SUM(o.total_amount), 0) as revenue_during_campaign
    FROM marketing.campaigns c
    LEFT JOIN sales.orders o ON o.order_date BETWEEN c.start_date AND c.end_date AND o.order_status = 'Delivered'
    GROUP BY c.campaign_id
)
SELECT 
    cs.campaign_id, cs.campaign_name, cs.start_date, cs.end_date,
    cs.end_date - cs.start_date as duration_days,
    ROUND(cs.budget::NUMERIC, 2) as budget,
    ROUND(cs.actual_spend::NUMERIC, 2) as actual_spend,
    cs.total_clicks, cs.total_conversions,
    CASE WHEN cs.total_clicks > 0 THEN ROUND((cs.total_conversions::NUMERIC / cs.total_clicks * 100), 2) ELSE 0 END as conversion_rate_pct,
    CASE WHEN cs.total_clicks > 0 THEN ROUND((cs.actual_spend / cs.total_clicks)::NUMERIC, 2) ELSE 0 END as cost_per_click,
    cr.orders_during_campaign,
    ROUND(cr.revenue_during_campaign::NUMERIC, 2) as attributed_revenue,
    CASE WHEN cs.actual_spend > 0 THEN ROUND(((cr.revenue_during_campaign - cs.actual_spend) / cs.actual_spend * 100)::NUMERIC, 2) ELSE 0 END as roi_pct,
    CASE 
        WHEN cs.actual_spend = 0 THEN 'Not Started'
        WHEN ((cr.revenue_during_campaign - cs.actual_spend) / NULLIF(cs.actual_spend, 0) * 100) >= 200 THEN 'Excellent'
        WHEN ((cr.revenue_during_campaign - cs.actual_spend) / NULLIF(cs.actual_spend, 0) * 100) >= 100 THEN 'Good'
        WHEN ((cr.revenue_during_campaign - cs.actual_spend) / NULLIF(cs.actual_spend, 0) * 100) >= 0 THEN 'Break Even'
        ELSE 'Losing Money'
    END as campaign_status
FROM campaign_spend cs
LEFT JOIN campaign_revenue cr ON cs.campaign_id = cr.campaign_id
ORDER BY cs.start_date DESC;

\echo '      ✓ View created: vw_campaign_performance'

\echo '[2/5] Creating view: vw_channel_performance...'

CREATE OR REPLACE VIEW analytics.vw_channel_performance AS
SELECT 
    a.channel,
    COUNT(DISTINCT a.campaign_id) as campaigns_using,
    ROUND(SUM(a.amount)::NUMERIC, 2) as total_spend,
    SUM(a.clicks) as total_clicks,
    SUM(a.conversions) as total_conversions,
    ROUND((SUM(a.amount) / NULLIF(SUM(a.clicks), 0))::NUMERIC, 2) as avg_cost_per_click,
    ROUND((SUM(a.conversions)::NUMERIC / NULLIF(SUM(a.clicks), 0) * 100), 2) as conversion_rate_pct,
    ROUND((SUM(a.amount) / SUM(SUM(a.amount)) OVER () * 100)::NUMERIC, 2) as pct_of_total_spend,
    RANK() OVER (ORDER BY SUM(a.conversions)::NUMERIC / NULLIF(SUM(a.clicks), 0) DESC NULLS LAST) as efficiency_rank
FROM marketing.ads_spend a
GROUP BY a.channel
ORDER BY total_spend DESC;

\echo '      ✓ View created: vw_channel_performance'

\echo '[3/5] Creating view: vw_promotion_effectiveness...'

CREATE OR REPLACE VIEW analytics.vw_promotion_effectiveness AS
WITH promo_sales AS (
    SELECT p.promo_id, p.promo_name, p.start_date, p.end_date, p.discount_percent,
        COUNT(DISTINCT o.order_id) as orders, SUM(o.total_amount) as revenue
    FROM products.promotions p
    LEFT JOIN sales.orders o ON o.order_date BETWEEN p.start_date AND p.end_date AND o.order_status = 'Delivered'
    GROUP BY p.promo_id, p.promo_name, p.start_date, p.end_date, p.discount_percent
)
SELECT promo_id, promo_name, start_date, end_date, 
    end_date - start_date as duration_days, discount_percent,
    COALESCE(orders, 0) as orders, ROUND(COALESCE(revenue, 0)::NUMERIC, 2) as revenue
FROM promo_sales ORDER BY start_date DESC;

\echo '      ✓ View created: vw_promotion_effectiveness'

\echo '[4/5] Creating view: vw_email_engagement...'

CREATE OR REPLACE VIEW analytics.vw_email_engagement AS
SELECT 
    c.campaign_id, c.campaign_name,
    DATE_TRUNC('month', e.sent_date)::DATE as send_month,
    COUNT(*) as emails_sent,
    COUNT(*) FILTER (WHERE e.opened) as emails_opened,
    COUNT(*) FILTER (WHERE e.clicked) as emails_clicked,
    ROUND((COUNT(*) FILTER (WHERE e.opened)::NUMERIC / NULLIF(COUNT(*), 0) * 100), 2) as open_rate_pct,
    ROUND((COUNT(*) FILTER (WHERE e.clicked)::NUMERIC / NULLIF(COUNT(*), 0) * 100), 2) as click_rate_pct
FROM marketing.email_clicks e
JOIN marketing.campaigns c ON e.campaign_id = c.campaign_id
GROUP BY c.campaign_id, c.campaign_name, DATE_TRUNC('month', e.sent_date)
ORDER BY send_month DESC;

\echo '      ✓ View created: vw_email_engagement'

\echo '[5/5] Creating materialized view: mv_marketing_roi...'

DROP MATERIALIZED VIEW IF EXISTS analytics.mv_marketing_roi CASCADE;

CREATE MATERIALIZED VIEW analytics.mv_marketing_roi AS
SELECT 
    (SELECT MAX(order_date) FROM sales.orders) as reference_date,
    COUNT(DISTINCT c.campaign_id) as total_campaigns,
    ROUND(SUM(c.budget)::NUMERIC, 2) as total_budget,
    ROUND(COALESCE(SUM(a.amount), 0)::NUMERIC, 2) as total_spend,
    COALESCE(SUM(a.clicks), 0) as total_clicks,
    COALESCE(SUM(a.conversions), 0) as total_conversions,
    ROUND((COALESCE(SUM(a.conversions), 0)::NUMERIC / NULLIF(SUM(a.clicks), 0) * 100), 2) as overall_conversion_rate,
    ROUND((COALESCE(SUM(a.amount), 0) / NULLIF(SUM(a.clicks), 0))::NUMERIC, 2) as overall_cpc
FROM marketing.campaigns c
LEFT JOIN marketing.ads_spend a ON c.campaign_id = a.campaign_id;

\echo '      ✓ Materialized view created: mv_marketing_roi'

-- JSON Functions
CREATE OR REPLACE FUNCTION analytics.get_marketing_summary_json() RETURNS JSON AS $$
BEGIN RETURN (SELECT row_to_json(t) FROM analytics.mv_marketing_roi t); END;
$$ LANGUAGE plpgsql STABLE;

CREATE OR REPLACE FUNCTION analytics.get_campaign_performance_json() RETURNS JSON AS $$
BEGIN RETURN (
    SELECT json_agg(json_build_object(
        'campaignName', campaign_name, 'budget', budget, 'spend', actual_spend,
        'clicks', total_clicks, 'conversions', total_conversions, 'roi', roi_pct, 'status', campaign_status
    ) ORDER BY start_date DESC)
    FROM analytics.vw_campaign_performance LIMIT 20
); END;
$$ LANGUAGE plpgsql STABLE;

CREATE OR REPLACE FUNCTION analytics.get_channel_performance_json() RETURNS JSON AS $$
BEGIN RETURN (
    SELECT json_agg(json_build_object(
        'channel', channel, 'spend', total_spend, 'clicks', total_clicks,
        'conversions', total_conversions, 'conversionRate', conversion_rate_pct, 'efficiencyRank', efficiency_rank
    ) ORDER BY total_spend DESC)
    FROM analytics.vw_channel_performance
); END;
$$ LANGUAGE plpgsql STABLE;

CREATE OR REPLACE FUNCTION analytics.get_email_engagement_json() RETURNS JSON AS $$
BEGIN RETURN (
    SELECT json_agg(json_build_object(
        'campaignName', campaign_name, 'sent', emails_sent, 'opened', emails_opened,
        'clicked', emails_clicked, 'openRate', open_rate_pct, 'clickRate', click_rate_pct
    ) ORDER BY send_month DESC)
    FROM analytics.vw_email_engagement LIMIT 20
); END;
$$ LANGUAGE plpgsql STABLE;

\echo '      ✓ JSON functions created (4 functions)'

REFRESH MATERIALIZED VIEW analytics.mv_marketing_roi;

\echo ''
\echo '============================================================================'
\echo '             MARKETING ANALYTICS MODULE - COMPLETE                          '
\echo '============================================================================'


