
-- Create transformed sales summary view
CREATE OR REPLACE VIEW v_sales_summary AS
SELECT
    s.sale_date,
    c.customer_name,
    p.product_name,
    s.quantity,
    s.unit_price,
    s.quantity * s.unit_price AS total_amount,
    c.city,
    p.category
FROM raw_sales s
JOIN raw_customers c ON s.customer_id = c.customer_id
JOIN raw_products p ON s.product_id = p.product_id;
