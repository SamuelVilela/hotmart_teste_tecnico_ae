-- Quais são os 50 maiores produtores em faturamento ($) de 2021? 
SELECT 
    p.producer_id,
    SUM(pi.puritem_quantity * pi.purchase_value) faturamento
FROM 
    purchase p
INNER JOIN 
    product_item pi 
ON 
    p.prod_item_id = pi.prod_item_id
AND p.prod_item_partition = pi.prod_item_partition

WHERE 
    p.order_date >= DATE '2021-01-01'
AND p.order_date <= DATE '2021-12-31'
AND p.release_date IS NOT NULL
GROUP BY 
    p.producer_id
ORDER BY 
    faturamento DESC
LIMIT 50