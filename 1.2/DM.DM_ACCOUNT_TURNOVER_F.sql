CREATE OR REPLACE FUNCTION dm.fill_account_turnover_f(i_OnDate DATE)
RETURNS VOID AS $$
BEGIN
 
    DELETE FROM 
		dm.dm_account_turnover_f
    WHERE 
		on_date = i_OnDate;

    
    INSERT INTO DM.DM_ACCOUNT_TURNOVER_F (on_date, account_rk, credit_amount, credit_amount_rub, debet_amount, debet_amount_rub)
    WITH t1 AS (
        SELECT 
            p."OPER_DATE"
            ,p."CREDIT_ACCOUNT_RK" AS account_rk
            ,b."CURRENCY_RK"
            ,SUM(p."CREDIT_AMOUNT") AS credit_amount
            ,0 AS debet_amount
        FROM 
			ds.ft_posting_f p
        LEFT JOIN ds.ft_balance_f b ON b."ACCOUNT_RK" = p."CREDIT_ACCOUNT_RK"
        WHERE 
			p."OPER_DATE" = i_OnDate
        GROUP BY 
			p."OPER_DATE"
			,p."CREDIT_ACCOUNT_RK"
			,b."CURRENCY_RK"
        UNION ALL
        SELECT 
            p."OPER_DATE"
            ,p."DEBET_ACCOUNT_RK" AS account_rk
            ,b."CURRENCY_RK"
            ,0 AS credit_amount
            ,SUM(p."DEBET_AMOUNT") AS debet_amount
        FROM 
			ds.ft_posting_f p
        LEFT JOIN ds.ft_balance_f b ON b."ACCOUNT_RK" = p."DEBET_ACCOUNT_RK"
        WHERE 
			p."OPER_DATE" = i_OnDate
        GROUP BY 
			p."OPER_DATE"
			,p."DEBET_ACCOUNT_RK"
			,b."CURRENCY_RK"
    	),
	    t2 AS (
	    SELECT 
			"CURRENCY_RK"
			,"REDUCED_COURCE" 
	    FROM 
			ds.md_exchange_rate_d
	    WHERE 
			i_OnDate BETWEEN "DATA_ACTUAL_DATE" AND "DATA_ACTUAL_END_DATE"
	    GROUP BY
			"CURRENCY_RK"
			,"REDUCED_COURCE" 
      	)
    SELECT 
		i_OnDate AS on_date
		,t1.account_rk
		,SUM(t1.credit_amount) AS credit_amount
		,SUM(t1.credit_amount * COALESCE(t2."REDUCED_COURCE", 1)) AS credit_amount_rub
		,SUM(t1.debet_amount) AS debet_amount
		,SUM(t1.debet_amount * COALESCE(t2."REDUCED_COURCE", 1)) AS debet_amount_rub
    FROM 
		t1
    LEFT JOIN t2 ON t1."CURRENCY_RK" = t2."CURRENCY_RK"
    GROUP BY 
		t1.account_rk;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE PROCEDURE dm.fill_account_turnover_procedure(start_date DATE, end_date DATE)
LANGUAGE plpgsql
AS $$
DECLARE
    currentDate DATE := start_date;
BEGIN
	WHILE currentDate <= end_date 
		LOOP
			PERFORM dm.fill_account_turnover_f(currentDate);
			currentDate := currentDate + INTERVAL '1 day';
		END LOOP;
END $$;

CALL dm.fill_account_turnover_procedure('2018-01-01', '2018-01-31');