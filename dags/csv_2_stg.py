from datetime import datetime, timedelta
from airflow.models import Variable
import pandas as pd
import vertica_python


def insert_to_stg(execution_date, **conn_info_vert):

    execution_date = datetime.strptime(execution_date, "%Y-%m-%dT%H:%M:%S%z").date() - timedelta(days=1)

    with vertica_python.connect(**conn_info_vert) as conn:
        curs = conn.cursor()
        insert_tr = f"""
        COPY transactions (operation_id, account_number_from, 
                    account_number_to, currency_code, country, status, transaction_type,
                    amount, transaction_dt) 
        FROM LOCAL \'/data//{str(execution_date)}//batch.csv\' DELIMITER \',\';
        """
        curs.execute(insert_tr)	
        conn.commit()

        insert_cur = f""" 
        COPY currencies (currency_code,currency_code_with,date_update,currency_with_div) 
        FROM LOCAL \'/data//{str(execution_date)}//currencies.csv\' DELIMITER \',\';"""

        curs.execute(insert_cur)	
        conn.commit()
