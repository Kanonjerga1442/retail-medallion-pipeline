SELECT
    country.country_name,
    SUM(sales.sales_amount)::numeric(18, 2) AS gross_sales,
    SUM(sales.adjustment_amount)::numeric(18, 2) AS adjustments,
    SUM(sales.net_amount)::numeric(18, 2) AS net_sales,
    COUNT(DISTINCT sales.invoice_no) AS invoice_count,
    COUNT(DISTINCT NULLIF(sales.customer_id, 'GUEST')) AS identified_customer_count
FROM gold.fact_sales AS sales
INNER JOIN gold.dim_country AS country
    ON country.country_key = sales.country_key
GROUP BY country.country_name
ORDER BY net_sales DESC;
