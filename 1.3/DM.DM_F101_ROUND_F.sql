CREATE OR REPLACE FUNCTION dm.fill_f101_round_f(i_OnDate date)
RETURNS VOID AS $$
DECLARE
  i_FromDate date := date_trunc('month', i_OnDate) - interval '1 month';
  i_ToDate date := i_FromDate + interval '1 month' - interval '1 day';
BEGIN
  -- Удаляем существующие данные за дату расчета
  DELETE FROM 
    dm.dm_f101_round_f
  WHERE 
    from_date = i_FromDate;
  -- Наполняем витрину данными
  INSERT INTO dm.dm_f101_round_f (from_date, chapter, ledger_account, balance_in_rub, balance_in_val, balance_in_total, turn_deb_rub, turn_deb_val, turn_deb_total, turn_cre_rub, turn_cre_val, turn_cre_total, balance_out_rub, balance_out_val, balance_out_total)
  WITH sum_balance_in AS (
    SELECT
      LEFT(ad."ACCOUNT_NUMBER", 5) AS ledger_account
      ,SUBSTRING(ad."ACCOUNT_NUMBER", 1, 1) AS chapter
      ,COALESCE(SUM(CASE WHEN ad."CURRENCY_CODE" IN ('810', '643') THEN ab.balance_out_rub ELSE 0 END), 0) AS balance_in_rub
      ,COALESCE(SUM(CASE WHEN ad."CURRENCY_CODE" NOT IN ('810', '643') THEN ab.balance_out_rub ELSE 0 END), 0) AS balance_in_val
      ,COALESCE(SUM(ab.balance_out_rub), 0) AS balance_in_total
    FROM
      ds.md_account_d ad
    LEFT JOIN dm.dm_account_balance_f ab ON ad."ACCOUNT_RK" = ab.account_rk 
    WHERE
      ab.on_date = i_FromDate - interval '1 day'
    GROUP BY
      LEFT(ad."ACCOUNT_NUMBER", 5)
    ,SUBSTRING(ad."ACCOUNT_NUMBER", 1, 1)
  ),
  sum_turnover_deb AS (
    SELECT
      LEFT(ad."ACCOUNT_NUMBER", 5) AS ledger_account
      ,COALESCE(SUM(CASE WHEN ad."CURRENCY_CODE" IN ('810', '643') THEN at.debet_amount_rub ELSE 0 END), 0) AS turn_deb_rub
      ,COALESCE(SUM(CASE WHEN ad."CURRENCY_CODE" NOT IN ('810', '643') THEN at.debet_amount_rub ELSE 0 END), 0) AS turn_deb_val
      ,COALESCE(SUM(at.debet_amount_rub), 0) AS turn_deb_total
    FROM
      ds.md_account_d ad
    LEFT JOIN dm.dm_account_turnover_f at ON ad."ACCOUNT_RK" = at.account_rk
    WHERE
      at.on_date BETWEEN i_FromDate AND i_ToDate
    GROUP BY
      LEFT(ad."ACCOUNT_NUMBER", 5)
  ),
  sum_turnover_cre AS (
    SELECT
      LEFT(ad."ACCOUNT_NUMBER", 5) AS ledger_account
      ,COALESCE(SUM(CASE WHEN ad."CURRENCY_CODE" IN ('810', '643') THEN at.credit_amount_rub ELSE 0 END), 0) AS turn_cre_rub
      ,COALESCE(SUM(CASE WHEN ad."CURRENCY_CODE" NOT IN ('810', '643') THEN at.credit_amount_rub ELSE 0 END), 0) AS turn_cre_val
      ,COALESCE(SUM(at.credit_amount_rub), 0) AS turn_cre_total
    FROM
      ds.md_account_d ad
    LEFT JOIN dm.dm_account_turnover_f at ON ad."ACCOUNT_RK" = at.account_rk 
    WHERE
      at.on_date BETWEEN i_FromDate AND i_ToDate
    GROUP BY
      LEFT(ad."ACCOUNT_NUMBER", 5)
  ),
  sum_balance_out AS (
    SELECT
      LEFT(ad."ACCOUNT_NUMBER", 5) AS ledger_account
      ,COALESCE(SUM(CASE WHEN ad."CURRENCY_CODE" IN ('810', '643') THEN ab.balance_out_rub ELSE 0 END), 0) AS balance_out_rub
      ,COALESCE(SUM(CASE WHEN ad."CURRENCY_CODE" NOT IN ('810', '643') THEN ab.balance_out_rub ELSE 0 END), 0) AS balance_out_val
      ,COALESCE(SUM(ab.balance_out_rub), 0)  AS balance_out_total
    FROM
      ds.md_account_d ad
    LEFT JOIN dm.dm_account_balance_f ab ON ad."ACCOUNT_RK" = ab.account_rk 
    WHERE
      ab.on_date = i_ToDate
    GROUP BY
      LEFT(ad."ACCOUNT_NUMBER", 5)
  )
  SELECT
      i_FromDate
      ,sb.chapter
      ,sb.ledger_account
      ,sb.balance_in_rub
      ,sb.balance_in_val
      ,sb.balance_in_total
      ,COALESCE(td.turn_deb_rub, 0)
      ,COALESCE(td.turn_deb_val, 0)
      ,COALESCE(td.turn_deb_total, 0)
      ,COALESCE(tc.turn_cre_rub, 0)
      ,COALESCE(tc.turn_cre_val, 0)
      ,COALESCE(tc.turn_cre_total, 0)
      ,COALESCE(so.balance_out_rub, 0)
      ,COALESCE(so.balance_out_val, 0)
      ,COALESCE(so.balance_out_total,0)
  FROM
    sum_balance_in sb
  LEFT JOIN sum_turnover_deb td ON sb.ledger_account = td.ledger_account
  LEFT JOIN sum_turnover_cre tc ON sb.ledger_account = tc.ledger_account
  LEFT JOIN sum_balance_out so ON sb.ledger_account = so.ledger_account;
END;
$$ LANGUAGE plpgsql;
    SELECT dm.fill_f101_round_f('2018-02-01'::date);
