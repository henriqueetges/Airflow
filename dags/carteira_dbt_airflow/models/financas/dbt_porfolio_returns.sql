{% set dates = get_min_max_date('inv.public.stock_quotes_history', 'date') %}
/* I WANT TO USE THIS MODEL TO GENERATE A RANGE OF DAYS AND BE ABLE TO CALCULATE RETURNS FOR EACH STOCK ON THAT DAY, BASED ON IT'S CLOSING PRICES */

SELECT 
    '{{ dates["min_date"] }}' as min_date
    , '{{ dates["max_date"] }}' as max_date 
