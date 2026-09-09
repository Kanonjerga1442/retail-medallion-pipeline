SELECT
    product.stock_code,
    product.product_name,
    SUM(sales.sales_amount)::numeric(18, 2) AS gross_sales,
    ABS(SUM(sales.adjustment_amount))::numeric(18, 2) AS adjustment_value,
    CASE
        WHEN SUM(sales.sales_amount) = 0 THEN NULL
        ELSE (
            ABS(SUM(sales.adjustment_amount))
            / SUM(sales.sales_amount)
        )::numeric(12, 4)
    END AS adjustment_rate,
    ABS(SUM(sales.quantity) FILTER (
        WHERE sales.transaction_type IN ('RETURN', 'CANCELLATION')
    ))::bigint AS adjusted_units
FROM gold.fact_sales AS sales
INNER JOIN gold.dim_product AS product
    ON product.product_key = sales.product_key
GROUP BY product.stock_code, product.product_name
HAVING SUM(sales.sales_amount) > 0
ORDER BY adjustment_rate DESC, adjustment_value DESC;
