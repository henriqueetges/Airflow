with totals as (
SELECT 
ticker, 
sum(quantity * price)::numeric AS total_position,
sum(quantity)::integer AS total_quantity
FROM inv.public.transac
GROUP BY ticker
having sum(quantity) > 0
)


SELECT 
ticker, 
ROUND(total_position / total_quantity, 2) as PM,
total_quantity::integer

FROM totals