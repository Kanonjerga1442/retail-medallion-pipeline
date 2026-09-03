SELECT
    product.product_name || ' (' || product.stock_code || ')' AS product,
    ABS(SUM(sales.quantity))::bigint AS returned_cancelled_units
FROM gold.fact_sales AS sales
INNER JOIN gold.dim_product AS product
    ON product.product_key = sales.product_key
WHERE sales.transaction_type IN ('RETURN', 'CANCELLATION')
GROUP BY product.product_name, product.stock_code
ORDER BY returned_cancelled_units DESC
LIMIT 10;
