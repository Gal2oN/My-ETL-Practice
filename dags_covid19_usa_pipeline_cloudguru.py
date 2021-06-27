import pandas as pd
import numpy as np

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.utils.dates import days_ago
from datetime import timedelta


# For PythonOperator

def data_ny():

    url = "https://raw.githubusercontent.com/nytimes/covid-19-data/master/us.csv"
    data_ny = pd.read_csv(url)
    data_ny['date'] = pd.to_datetime(data_ny['date'])
    data_ny.to_csv("/home/airflow/gcs/data/data_ny.csv", index=False)

    

def data_jh():
    url = "https://raw.githubusercontent.com/datasets/covid-19/master/data/time-series-19-covid-combined.csv"
    data_jh = pd.read_csv(url)
    data_jh_usa = data_jh[data_jh['Country/Region'] == 'US']
    data_jh_usa_recovered = data_jh_usa.drop(['Confirmed', 'Deaths', 'Province/State', 'Country/Region'], 1)
    data_jh_usa_recovered.to_csv("/home/airflow/gcs/data/data_jh.csv", index=False)
    

def data_ny_jh_merge():
    data_ny = pd.read_csv("/home/airflow/gcs/data/data_ny.csv")
    data_jh = pd.read_csv("/home/airflow/gcs/data/data_jh.csv")
    
    data_ny_jh = data_ny.merge(data_jh, how="left", left_on="date", right_on="Date")
    data_ny_jh.dropna(subset = ["Recovered"], inplace=True)
    data_ny_jh.drop (['Date'], 1, inplace=True)
    data_ny_jh['Recovered'] = data_ny_jh['Recovered'].astype(np.int64)
    data_ny_jh.to_csv("/home/airflow/gcs/data/covid19_ny_jh.csv", index=False)


# Default Args

default_args = {
    'owner': 'Narong.H',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['airflow1@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'schedule_interval': '@once',
}

# Create DAG

dag = DAG(
    'Covid19_USA_to_bq_powerbi',
    default_args=default_args,
    description='Cloud Guru Challenge with Covid19 in USA to Bigquery and Power Bi',
    schedule_interval='0 8 * * *',
)

# Tasks

# get data from new york times
t1 = PythonOperator(
    task_id ='data_from_ny',
    python_callable = data_ny,
    dag = dag,
)

# get data from john hopkins
t2 = PythonOperator(
    task_id ='data_from_jh',
    python_callable = data_jh,
    dag = dag,
)

# merge and clean data
t3 = PythonOperator(
    task_id = 'merge_and_clean',
    python_callable = data_ny_jh_merge,
    dag = dag,
)

t4 = BashOperator(
    task_id = 'load_bq',
    bash_command = 'bq load --source_format=CSV --autodetect\
                    Covid19_USA.Covid19_USA_NYJH\
                    gs://australia-southeast1-cloudg-5567f3e3-bucket/data/covid19_ny_jh.csv',
    dag = dag,
)

# Dependencies

[t1, t2] >> t3 >> t4
