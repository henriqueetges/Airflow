WITH owned AS (SELECT DISTINCT ticker FROM inv.public.transac)

SELECT 
i.*, 
    CASE WHEN o.ticker IS NOT NULL THEN 1 ELSE 0 END AS owned 

FROM inv.public.stock_info I
LEFT JOIN owned O
    ON I.ticker = O.ticker

