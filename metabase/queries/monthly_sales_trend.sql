SELECT
    DATE_TRUNC('month', date_dimension.full_date)::date AS sales_month,
    SUM(sales.sales_amount)::numeric(18, 2) AS gross_sales,
    SUM(sales.adjustment_amount)::numeric(18, 2) AS adjustments,
    SUM(sales.net_amount)::numeric(18, 2) AS net_sales,
    COUNT(DISTINCT sales.invoice_no) AS invoice_count,
    COUNT(DISTINCT NULLIF(sales.customer_id, 'GUEST')) AS identified_customer_count
FROM gold.fact_sales AS sales
INNER JOIN gold.dim_date AS date_dimension
    ON date_dimension.date_key = sales.date_key
GROUP BY DATE_TRUNC('month', date_dimension.full_date)
ORDER BY sales_month;
