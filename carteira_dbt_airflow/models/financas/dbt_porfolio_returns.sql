/*
 CALCULATES FOR EACH OF THE TICKERS THE PM, THE POSITIONS AT THE DAY AND THE ESTIMATE PROFITS AT THAT DAY
 */

SELECT
    pm.*
    , (sh.open * pm.total_quantity)::numeric AS position_open
    , (sh.close * pm.total_quantity)::numeric AS position_close
    , (sh.high* pm.total_quantity)::numeric AS position_high
	, pm.total_position - (sh.open * pm.total_quantity)::numeric AS pl_open
	, pm.total_position - (sh.close * pm.total_quantity)::numeric AS pl_close
	, pm.total_position - (sh.high * pm.total_quantity)::numeric AS pl_high
FROM dbt.dbt_calculate_pm pm
INNER JOIN inv.public.stock_quotes_history SH
        ON SH.date::date = pm.dt::date
        AND SH.ticker = PM.ticker
