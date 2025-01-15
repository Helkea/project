
--Преобразования импортированной таблицы ds.ft_balance_f
--Добавление условия NOT NULL
ALTER TABLE ds.ft_balance_f ALTER COLUMN "ON_DATE" SET NOT NULL;
ALTER TABLE ds.ft_balance_f ALTER COLUMN "ACCOUNT_RK" SET NOT NULL;
--Добавление первичного ключа
ALTER TABLE ds.ft_balance_f ADD PRIMARY KEY ("ON_DATE", "ACCOUNT_RK");
--Изменение типов данных в атрибутах
ALTER TABLE ds.ft_balance_f ALTER COLUMN "ON_DATE" TYPE date USING to_date("ON_DATE", 'DD MM YYYY');
ALTER TABLE ds.ft_balance_f ALTER COLUMN "ACCOUNT_RK" TYPE int;
ALTER TABLE ds.ft_balance_f ALTER COLUMN "CURRENCY_RK" TYPE int;
ALTER TABLE ds.ft_balance_f ALTER COLUMN "BALANCE_OUT" TYPE double precision;

--Преобразования импортированной таблицы ds.ft_posting_f
--Добавление условия NOT NULL
ALTER TABLE ds.ft_posting_f ALTER COLUMN "OPER_DATE" SET NOT NULL;
ALTER TABLE ds.ft_posting_f ALTER COLUMN "CREDIT_ACCOUNT_RK" SET NOT NULL;
ALTER TABLE ds.ft_posting_f ALTER COLUMN "DEBET_ACCOUNT_RK" SET NOT NULL;
--Изменение типов данных в атрибутах
ALTER TABLE ds.ft_posting_f ALTER COLUMN "OPER_DATE" TYPE date USING to_date("OPER_DATE", 'DD MM YYYY');
ALTER TABLE ds.ft_posting_f ALTER COLUMN "CREDIT_ACCOUNT_RK" TYPE int;
ALTER TABLE ds.ft_posting_f ALTER COLUMN "DEBET_ACCOUNT_RK" TYPE int;
ALTER TABLE ds.ft_posting_f ALTER COLUMN "CREDIT_AMOUNT" TYPE double precision;
ALTER TABLE ds.ft_posting_f ALTER COLUMN "DEBET_AMOUNT" TYPE double precision;

--Преобразования импортированной таблицы ds.md_account_d
--Добавление условия NOT NULL
ALTER TABLE ds.md_account_d ALTER COLUMN "DATA_ACTUAL_DATE" SET NOT NULL;
ALTER TABLE ds.md_account_d ALTER COLUMN "DATA_ACTUAL_END_DATE" SET NOT NULL;
ALTER TABLE ds.md_account_d ALTER COLUMN "ACCOUNT_RK" SET NOT NULL;
ALTER TABLE ds.md_account_d ALTER COLUMN "ACCOUNT_NUMBER" SET NOT NULL;
ALTER TABLE ds.md_account_d ALTER COLUMN "CHAR_TYPE" SET NOT NULL;
ALTER TABLE ds.md_account_d ALTER COLUMN "CURRENCY_RK" SET NOT NULL;
ALTER TABLE ds.md_account_d ALTER COLUMN "CURRENCY_CODE" SET NOT NULL;
--Добавление первичного ключа
ALTER TABLE ds.md_account_d ADD PRIMARY KEY ("DATA_ACTUAL_DATE", "ACCOUNT_RK");
--Изменение типов данных в атрибутах
ALTER TABLE ds.md_account_d ALTER COLUMN "DATA_ACTUAL_DATE" TYPE date USING to_date("DATA_ACTUAL_DATE", 'YYYY MM DD');
ALTER TABLE ds.md_account_d ALTER COLUMN "DATA_ACTUAL_END_DATE" TYPE date USING to_date("DATA_ACTUAL_END_DATE", 'YYYY MM DD');
ALTER TABLE ds.md_account_d ALTER COLUMN "ACCOUNT_RK" TYPE int;
ALTER TABLE ds.md_account_d ALTER COLUMN "ACCOUNT_NUMBER" TYPE varchar(20);
ALTER TABLE ds.md_account_d ALTER COLUMN "CHAR_TYPE" TYPE varchar(1);
ALTER TABLE ds.md_account_d ALTER COLUMN "CURRENCY_RK" TYPE int;
ALTER TABLE ds.md_account_d ALTER COLUMN "CURRENCY_CODE" TYPE varchar(3);

--Преобразования импортированной таблицы ds.md_currency_d
--Добавление условия NOT NULL
ALTER TABLE ds.md_currency_d ALTER COLUMN "CURRENCY_RK" SET NOT NULL;
ALTER TABLE ds.md_currency_d ALTER COLUMN "DATA_ACTUAL_DATE" SET NOT NULL;
--Добавление первичного ключа
ALTER TABLE ds.md_currency_d ADD PRIMARY KEY ("CURRENCY_RK", "DATA_ACTUAL_DATE");
--Изменение типов данных в атрибутах
ALTER TABLE ds.md_currency_d ALTER COLUMN "CURRENCY_RK" TYPE int;
ALTER TABLE ds.md_currency_d ALTER COLUMN "DATA_ACTUAL_DATE" TYPE date USING to_date("DATA_ACTUAL_DATE", 'DD MM YYYY');
ALTER TABLE ds.md_currency_d ALTER COLUMN "DATA_ACTUAL_END_DATE" TYPE date USING to_date("DATA_ACTUAL_END_DATE", 'DD MM YYYY');
ALTER TABLE ds.md_currency_d ALTER COLUMN "CURRENCY_CODE" TYPE varchar(3);
ALTER TABLE ds.md_currency_d ALTER COLUMN "CODE_ISO_CHAR" TYPE varchar(3);

--Преобразования импортированной таблицы ds.md_exchange_rate_d
--Добавление условия NOT NULL
ALTER TABLE ds.md_exchange_rate_d ALTER COLUMN "DATA_ACTUAL_DATE" SET NOT NULL;
ALTER TABLE ds.md_exchange_rate_d ALTER COLUMN "CURRENCY_RK" SET NOT NULL;
--Добавление первичного ключа
ALTER TABLE ds.md_exchange_rate_d ADD PRIMARY KEY ("CURRENCY_RK", "DATA_ACTUAL_DATE");
--Изменение типов данных в атрибутах
ALTER TABLE ds.md_exchange_rate_d ALTER COLUMN "DATA_ACTUAL_DATE" TYPE date USING to_date("DATA_ACTUAL_DATE", 'YYYY MM DD');


ALTER TABLE ds.md_exchange_rate_d 
ALTER COLUMN "DATA_ACTUAL_END_DATE" TYPE date 
USING to_date(CAST("DATA_ACTUAL_END_DATE" AS TEXT), 'YYYY-MM-DD');



ALTER TABLE ds.md_exchange_rate_d ALTER COLUMN "CURRENCY_RK" TYPE int;
ALTER TABLE ds.md_exchange_rate_d ALTER COLUMN "REDUCED_COURCE" TYPE double precision;
ALTER TABLE ds.md_exchange_rate_d ALTER COLUMN "CODE_ISO_NUM" TYPE varchar(3);

--Преобразования импортированной таблицы ds.md_ledger_account_s
--Добавление условия NOT NULL
ALTER TABLE ds.md_ledger_account_s ALTER COLUMN "LEDGER_ACCOUNT" SET NOT NULL;
ALTER TABLE ds.md_ledger_account_s ALTER COLUMN "START_DATE" SET NOT NULL;
--Добавление первичного ключа
ALTER TABLE ds.md_ledger_account_s ADD PRIMARY KEY ("LEDGER_ACCOUNT", "START_DATE");
--Изменение типов данных в атрибутах
ALTER TABLE ds.md_ledger_account_s ALTER COLUMN "CHAPTER" TYPE char(1);
ALTER TABLE ds.md_ledger_account_s ALTER COLUMN "CHAPTER_NAME" TYPE varchar(16);
ALTER TABLE ds.md_ledger_account_s ALTER COLUMN "SECTION_NUMBER" TYPE int;
ALTER TABLE ds.md_ledger_account_s ALTER COLUMN "SECTION_NAME" TYPE varchar(22);
ALTER TABLE ds.md_ledger_account_s ALTER COLUMN "SUBSECTION_NAME" TYPE varchar(21);
ALTER TABLE ds.md_ledger_account_s ALTER COLUMN "LEDGER1_ACCOUNT" TYPE int;
ALTER TABLE ds.md_ledger_account_s ALTER COLUMN "LEDGER1_ACCOUNT_NAME" TYPE varchar(47);
ALTER TABLE ds.md_ledger_account_s ALTER COLUMN "LEDGER_ACCOUNT" TYPE int;
ALTER TABLE ds.md_ledger_account_s ALTER COLUMN "LEDGER_ACCOUNT_NAME" TYPE varchar(153);
ALTER TABLE ds.md_ledger_account_s ALTER COLUMN "CHARACTERISTIC" TYPE char(1);
ALTER TABLE ds.md_ledger_account_s ALTER COLUMN "START_DATE" TYPE date USING to_date("START_DATE", 'YYYY MM DD');
ALTER TABLE ds.md_ledger_account_s ALTER COLUMN "END_DATE" TYPE date USING to_date("END_DATE", 'YYYY MM DD');
--Добавление столбцов в соответствии с требуемой структурой сущности
ALTER TABLE ds.md_ledger_account_s ADD COLUMN "IS_RESIDENT" int;
ALTER TABLE ds.md_ledger_account_s ADD COLUMN "IS_RESERVE" int;
ALTER TABLE ds.md_ledger_account_s ADD COLUMN "IS_RESERVED" int;
ALTER TABLE ds.md_ledger_account_s ADD COLUMN "IS_LOAN" int;
ALTER TABLE ds.md_ledger_account_s ADD COLUMN "IS_RESERVED_ASSETS" int;
ALTER TABLE ds.md_ledger_account_s ADD COLUMN "IS_OVERDUE" int;
ALTER TABLE ds.md_ledger_account_s ADD COLUMN "IS_INTEREST" int;
ALTER TABLE ds.md_ledger_account_s ADD COLUMN "PAIR_ACCOUNT" varchar(5);
ALTER TABLE ds.md_ledger_account_s ADD COLUMN "IS_RUB_ONLY" int;
ALTER TABLE ds.md_ledger_account_s ADD COLUMN "MIN_TERM" varchar(1);
ALTER TABLE ds.md_ledger_account_s ADD COLUMN "MIN_TERM_MEASURE" varchar(1);
ALTER TABLE ds.md_ledger_account_s ADD COLUMN "MAX_TERM" varchar(1);
ALTER TABLE ds.md_ledger_account_s ADD COLUMN "MAX_TERM_MEASURE" varchar(1);
ALTER TABLE ds.md_ledger_account_s ADD COLUMN "LEDGER_ACC_FULL_NAME_TRANSLIT" varchar(1);
ALTER TABLE ds.md_ledger_account_s ADD COLUMN "IS_REVALUATION" varchar(1);
ALTER TABLE ds.md_ledger_account_s ADD COLUMN "IS_CORRECT" varchar(1);