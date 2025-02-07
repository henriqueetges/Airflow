with totals as (
SELECT 
ticker, 
sum(quantity * price)::double precision AS total_position,
sum(quantity)::integer AS total_quantity
FROM inv.public.transac
GROUP BY ticker
having sum(quantity) > 0
)


SELECT 
ticker, 
total_position / total_quantity::double precision as PM,
total_quantity::integer

FROM totals