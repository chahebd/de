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
SEGMENTED BY HASH(transaction_dt, operation_id) ALL NODES;


create table HZBASILYANDEXRU__STAGING.currencies (
	date_update TIMESTAMP(3) NOT NULL,
	currency_code int NOT NULL,
	currency_code_with int NOT NULL,
	currency_with_div numeric(5,2) NOT NULL
);


CREATE TABLE IF NOT EXISTS HZBASILYANDEXRU__DWH.global_metrics (
    date_update date NOT NULL,
    currency_from int NOT NULL,
    amount_total numeric(18, 2) NOT NULL,
    cnt_transactions int NOT NULL,
    avg_transactions_per_account numeric(18, 2) NOT NULL,
    cnt_accounts_make_transactions int NOT NULL
)
order by
    currency_from SEGMENTED BY HASH(date_update, currency_from) all nodes PARTITION BY date_update;