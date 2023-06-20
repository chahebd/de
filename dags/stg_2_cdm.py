from datetime import datetime, timedelta
from airflow.models import Variable
import vertica_python


def insert_to_cdm(execution_date, **conn_info_vert):

    execution_date = datetime.strptime(execution_date, "%Y-%m-%dT%H:%M:%S%z").date() - timedelta(days=1)

    with vertica_python.connect(**conn_info_vert) as conn:
        curs = conn.cursor()
        insert_cdm = f"""
insert into HZBASILYANDEXRU__DWH.global_metrics(date_update,
												 currency_from,
												 amount_total,
												 cnt_transactions,
												 avg_transactions_per_account,
												 cnt_accounts_make_transactions)
select 
  tr.date_update, 
  tr.currency_from, 
  cnt_transactions * COALESCE(cur_usd.currency_with_div, 1) as amount_total, 
  tr.cnt_transactions, 
  tr.cnt_transactions / tr.cnt_accounts_make_transactions as avg_transactions_per_account, 
  tr.cnt_accounts_make_transactions 
from 
  (
    select 
      transaction_dt :: date as date_update, 
      currency_code as currency_from, 
      sum(amount) as cnt_transactions, 
      count(DISTINCT account_number_to) as cnt_accounts_make_transactions 
    from 
      HZBASILYANDEXRU__STAGING.transactions 
    where 
      account_number_from != -1 
      and status = 'done' 
      and transaction_dt :: date = '{execution_date}' 
    group by 
      transaction_dt :: date, 
      currency_code
  ) as tr 
  left join (
    select 
      date_update, 
      currency_code, 
      currency_code_with, 
      currency_with_div 
    from 
      HZBASILYANDEXRU__STAGING.currencies c 
    where 
      currency_code_with = 420 
      and date_update :: date = '{execution_date}'
  ) as cur_usd on tr.currency_from = cur_usd.currency_code
        """
        curs.execute(insert_cdm)	
        conn.commit()
