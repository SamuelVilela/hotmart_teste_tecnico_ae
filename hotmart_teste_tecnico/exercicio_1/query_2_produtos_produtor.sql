-- quais são os 2 produtos que mais faturaram ($) de cada produtor? 
SELECT
    producer_id,
    product_id,
    faturamento
FROM (
    SELECT
        p.producer_id,
        pi.product_id,
        SUM(pi.purchase_value) as faturamento,
        ROW_NUMBER() OVER (
            PARTITION BY p.producer_id
            ORDER BY SUM(pi.purchase_value) DESC
        ) AS rn
    
    FROM 
		purchase p
	INNER JOIN 
		product_item pi 
	ON 
		p.prod_item_id = pi.prod_item_id
	AND p.prod_item_partition = pi.prod_item_partition
    WHERE p.release_date IS NOT NULL
    GROUP BY
        p.producer_id,
        pi.product_id
)
WHERE rn <= 2
ORDER BY producer_id, faturamento DESC