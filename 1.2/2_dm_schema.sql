CREATE SCHEMA IF NOT EXISTS DM


DROP TABLE IF EXISTS dm.dm_account_turnover_f;
CREATE TABLE IF NOT EXISTS dm.dm_account_turnover_f(
	on_date DATE
	,account_rk BIGINT
	,credit_amount NUMERIC(23,8)
	,credit_amount_rub NUMERIC(23,8)
	,debet_amount NUMERIC(23,8)
	,debet_amount_rub NUMERIC(23,8)
	,PRIMARY KEY (on_date, account_rk)
);

CREATE TABLE IF NOT EXISTS dm.dm_account_balance_f(
	on_date date
	,account_rk int
	,balance_out double precision
	,balance_out_rub double precision
);

ALTER TABLE dm.dm_account_balance_f ADD PRIMARY KEY (on_date, account_rk);
ALTER TABLE dm.dm_account_balance_f ALTER COLUMN on_date SET NOT NULL;
ALTER TABLE dm.dm_account_balance_f ALTER COLUMN account_rk SET NOT NULL;

