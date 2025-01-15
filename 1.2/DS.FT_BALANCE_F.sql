	--Наполняем таблицу dm.dm_account_balance_f данными за 31.12.2017
INSERT INTO dm.dm_account_balance_f(on_date, account_rk, balance_out, balance_out_rub)
	SELECT DISTINCT
		b."ON_DATE"
		,b."ACCOUNT_RK"
		,b."BALANCE_OUT"
		,(b."BALANCE_OUT" * COALESCE(e."REDUCED_COURCE", 1)) AS balance_out_rub
	FROM 
		ds.ft_balance_f b
	LEFT JOIN ds.md_exchange_rate_d e ON b."CURRENCY_RK" = e."CURRENCY_RK" AND b."ON_DATE" BETWEEN e."DATA_ACTUAL_DATE" AND e."DATA_ACTUAL_END_DATE"
	ORDER BY 
		b."ACCOUNT_RK"
		

CREATE OR REPLACE FUNCTION dm.fill_account_balance_f(i_OnDate date)
RETURNS VOID AS $$
BEGIN
  -- Удаляем существующие данные за дату расчета
    DELETE FROM 
    dm.dm_account_balance_f
    WHERE 
    on_date = i_OnDate;
    
  -- Вставляем рассчитанные данные
  INSERT INTO dm.dm_account_balance_f (on_date, account_rk, balance_out, balance_out_rub)
  WITH previous_day_balance AS (
      SELECT
          account_rk,
          on_date,
          balance_out,
          balance_out_rub
      FROM
          dm.dm_account_balance_f
      WHERE
          on_date = i_OnDate - interval '1 day'
    ),
    current_day_balance AS (
        SELECT
            account_rk
            ,on_date
            ,SUM(debet_amount) AS total_debet_amount
            ,SUM(credit_amount) AS total_credit_amount
            ,SUM(debet_amount_rub) AS total_debet_amount_rub
            ,SUM(credit_amount_rub) AS total_credit_amount_rub
        FROM
            dm.dm_account_turnover_f
        WHERE
            on_date = i_OnDate
        GROUP BY
            account_rk
            ,on_date
    )
    SELECT
        i_OnDate AS on_date
        ,a."ACCOUNT_RK"
        -- Расчет balance_out
        ,CASE 
            WHEN a."CHAR_TYPE" = 'А' THEN 
                COALESCE(p.balance_out, 0)
                + COALESCE(t.total_debet_amount, 0)
                - COALESCE(t.total_credit_amount, 0)
            WHEN a."CHAR_TYPE" = 'П' THEN 
                COALESCE(p.balance_out, 0)
                - COALESCE(t.total_debet_amount, 0)
                + COALESCE(t.total_credit_amount, 0)
        END AS balance_out
        -- Расчет balance_out_rub
        ,CASE 
            WHEN a."CHAR_TYPE" = 'А' THEN 
                COALESCE(p.balance_out_rub, 0)
                + COALESCE(t.total_debet_amount_rub, 0)
                - COALESCE(t.total_credit_amount_rub, 0)
            WHEN a."CHAR_TYPE" = 'П' THEN 
                COALESCE(p.balance_out_rub, 0)
                - COALESCE(t.total_debet_amount_rub, 0)
                + COALESCE(t.total_credit_amount_rub, 0)
        END AS balance_out_rub
    FROM
        ds.md_account_d a
    LEFT JOIN previous_day_balance p ON a."ACCOUNT_RK" = p.account_rk
    LEFT JOIN current_day_balance t ON a."ACCOUNT_RK" = t.account_rk
    WHERE
        i_OnDate BETWEEN a."DATA_ACTUAL_DATE" AND a."DATA_ACTUAL_END_DATE";
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE PROCEDURE dm.fill_account_balance_f(start_date date, end_date date)
LANGUAGE plpgsql
AS $$
DECLARE
    currentDate date := start_date;
BEGIN
	WHILE currentDate <= end_date 
		LOOP
			PERFORM dm.fill_account_balance_f(currentDate);
			currentDate := currentDate + interval '1 day';
		END LOOP;
END $$;

CALL dm.fill_account_balance_f('2018-01-01', '2018-01-31');