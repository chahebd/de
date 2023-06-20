from datetime import datetime


from airflow.decorators import dag, task
from airflow.models import Variable

from s3_2_csv import save_batches, save_currencies
from csv_2_stg import insert_to_stg
from stg_2_cdm import insert_to_cdm


AWS_ACCESS_KEY_ID = Variable.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = Variable.get("AWS_SECRET_ACCESS_KEY")


conn_info_vert = {'host': Variable.get("host"), 
                'port': Variable.get("port"),
                'user': Variable.get("user"),       
                'password': Variable.get("password"),
                #'database': Variable.get("database"),
                'autocommit': True
}

@dag(
    schedule_interval="@daily",
    start_date=datetime(2022, 10, 2),
    end_date=datetime(2022, 10, 31),
    catchup=True,
    concurrency=1,
    tags=['final_proj'],
)
def load_data_dag():

    @task()
    def save_b(execution_date):
        save_batches(execution_date)

    @task()
    def save_c(execution_date):
        save_currencies(execution_date)

    @task()
    def insert_stg(execution_date):
        insert_to_stg(execution_date, **conn_info_vert)

    @task()
    def insert_cdm(execution_date):
        insert_to_cdm(execution_date, **conn_info_vert)

    load_b = save_b(execution_date="{{ execution_date }}")
    load_c = save_c(execution_date="{{ execution_date }}")
    insert_s = insert_stg(execution_date="{{ execution_date }}")
    insert_c = insert_cdm(execution_date="{{ execution_date }}")

    load_b >> load_c >> insert_s >> insert_c # type: ignore


data_dag = load_data_dag()