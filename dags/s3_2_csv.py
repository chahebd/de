from datetime import datetime, timedelta

from airflow.models import Variable

import os
import boto3
import pandas as pd

AWS_ACCESS_KEY_ID = Variable.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = Variable.get("AWS_SECRET_ACCESS_KEY")


def get_s3_client():
    session = boto3.session.Session()
    s3_client = session.client(service_name='s3',
                               endpoint_url='https://storage.yandexcloud.net',
                               aws_access_key_id=AWS_ACCESS_KEY_ID,
                               aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    return s3_client


def save_batches(execution_date):
#инкрементальная загрузка батчей
    os.makedirs('/data/batches', exist_ok=True)
    files_names = os.listdir('/data/batches')

    execution_date = datetime.strptime(execution_date, "%Y-%m-%dT%H:%M:%S%z").date() - timedelta(days=1)
    df = pd.DataFrame()

    s3_client = get_s3_client()

    if len(files_names) != 0:
        batch_num = max([int(name[19:-4]) for name in files_names])
    else:
        batch_num = 1
    
    for i in range(batch_num, 11):
        s3_client.download_file(Bucket='final-project',
                                    Key=f'transactions_batch_{i}.csv',
                                    Filename=f'/data/batches/transactions_batch_{i}.csv')
        
        df_batch = pd.read_csv(f'/data/batches/transactions_batch_{i}.csv')
        df_batch['date'] = pd.to_datetime(df_batch['transaction_dt']).dt.date
        df = pd.concat([df, df_batch[df_batch['date'] == execution_date]])

        if execution_date < df_batch['date'].max():
            os.makedirs(f'/data/{execution_date}', exist_ok=True)
            df = df.drop_duplicates().drop('date', axis=1)
            df.to_csv(f'/data/{execution_date}/batch.csv', index=False)
            break


def save_currencies(execution_date):
    # Инкрементальная загрузка курса валют
    os.makedirs('/data/currencies', exist_ok=True)
    execution_date = datetime.strptime(execution_date, "%Y-%m-%dT%H:%M:%S%z").date() - timedelta(days=1)

    s3_client = get_s3_client()
    
    s3_client.download_file(Bucket='final-project',
                            Key='currencies_history.csv',
                            Filename='/data/currencies/currencies.csv')
    
    df_cur = pd.read_csv('/data/currencies/currencies.csv')
    df_cur['date'] = pd.to_datetime(df_cur['date_update']).dt.date
    df = df_cur[df_cur['date'] == execution_date]
    df = df.drop('date', axis=1)
    df.to_csv(f'/data/{execution_date}/currencies.csv', index=False)
