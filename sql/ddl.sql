create table HZBASILYANDEXRU__STAGING.transactions (
	operation_id uuid NOT NULL,
	account_number_from int NOT NULL,
	account_number_to int NOT NULL,
	currency_code int NOT NULL,
	country varchar NOT NULL,
	status varchar NOT NULL,
	transaction_type varchar(20) NOT NULL,
	amount int NOT NULL,
	transaction_dt TIMESTAMP(3) NOT NULL
	)
order by transaction_dt
segmented by hash(transaction_dt, operation_id) 
all nodes;


create projection transactions_date_projection
(
    operation_id,
    account_number_from,
    account_number_to,
    currency_code,
    country,
    status,
    transaction_type,
    amount
)
as
select
    operation_id,
    account_number_from,
    account_number_to,
    currency_code,
    country,
    status,
    transaction_type,
    amount
from transactions
order by transaction_dt
segmented by hash(transaction_dt)
all nodes;


create table HZBASILYANDEXRU__STAGING.currencies (
	date_update TIMESTAMP(3) NOT NULL,
	currency_code int NOT NULL,
	currency_code_with int NOT NULL,
	currency_with_div numeric(5,2) NOT NULL
)
order by date_update
segmented by hash(date_update, currency_code)
all nodes;


create projection currency_rates_date_projection
(
    currency_code,
    currency_code_with,
    currency_with_div
)
as
select
    currency_code,
    currency_code_with,
    currency_with_div
from currency_rates
order by date_update
segmented by hash(date_update)
all nodes;


create table if not exists HZBASILYANDEXRU__DWH.global_metrics (
    date_update date NOT NULL,
    currency_from int NOT NULL,
    amount_total numeric(18, 2) NOT NULL,
    cnt_transactions int NOT NULL,
    avg_transactions_per_account numeric(18, 2) NOT NULL,
    cnt_accounts_make_transactions int NOT NULL
)
order by currency_from 
segmented by hash(date_update, currency_from) 
all nodes partition by date_update;