{% set dates = get_min_max_date('inv.public.stock_quotes_history', 'date') %}


with totals as (
/* CALCULATES THE TOTAL TRANSACTION FOR EACH DAY AND EACH TICKER */
SELECT 
transac_date,
ticker, 
sum(quantity * price)::numeric AS total_position,
sum(quantity)::integer AS total_quantity
FROM inv.public.transac
GROUP BY ticker, transac_date
),

interval as (
    /* CALCULATES A PERIOD BETWEEN MY FIRST TRANSACTION THE LAST PRICE I HAVE CAPTURED */
    SELECT
        date_trunc('day', dd)::date AS dt
    FROM generate_series(
        (SELECT MIN(transac_date) from inv.public.transac)::timestamp
        , '{{ dates["max_date"] }}'::timestamp
        , '1 day'::interval) dd)





SELECT
I.dt, 
ticker, 
sum(total_position)::numeric  AS total_position,
sum(total_quantity)::numeric  AS total_quantity,
(sum(total_position)  / nullif(sum(total_quantity), 0))::numeric  AS pm

FROM interval I
LEFT JOIN totals T
    ON I.dt >= T.transac_date
group by 1, 2
HAVING SUM(total_quantity) > 0
