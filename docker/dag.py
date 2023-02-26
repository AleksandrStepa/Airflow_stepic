import pandas as pd
import sqlite3

CON = sqlite3.connect("example.db")

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python_operator import PythonOperator

# Выгрузка данных с сайта, запись в временный локальный файл
def extract_data(url, tmp_file, **context) -> pd.DataFrame:
    """ Extract CSV
    """
    pd.read_csv(url).to_csv(tmp_file)

# Группировка данных
def transform_data(group, agreg, tmp_file, tmp_agg_file, **context) -> pd.DataFrame:
    """ Group by data
    """
    data = pd.read_csv(tmp_file)
    data.groupby(group).agg(agreg).reset_index().to_csv(tmp_agg_file)

# Загрузка в базу данных
def load_data(tmp_file, table_name, conn=CON, **context) -> None:
    """ Load to DB
    """
    data = pd.read_csv(tmp_file)
    data["insert_time"] = pd.to_datetime("now")
    data.to_sql(table_name, conn, if_exists='replace', index=False)

dag = DAG(dag_id='dag',
          default_args={'owner':'airflow'},
          schedule_interval='@daily',
          start_date=days_ago(1))

extract_data = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    op_kwargs={
        'url':'https://raw.githubusercontent.com/dm-novikov/stepik_airflow_course/main/data/data.csv',
        'tmp_file':'/tmp/file.csv'
    },
    dag=dag
)

transform_data = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    op_kwargs={
        'tmp_file':'/tmp/file.csv',
        'tmp_agg_file':'/tmp/file_agg.csv',
        'group':['A', 'B', 'C'],
        'agreg':{"D":sum}
    },
    dag=dag,
)

load_data = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    op_kwargs={
        'tmp_file':'/tmp/file.csv',
        'table_name':'table'
    },
    dag=dag
)

email_op = EmailOperator(
    task_id='send_email',
    to="by-step1987@yandex.ru",
    subject="Test, please ignore",
    html_content=None,
    files=['/tmp/file_agg.csv']
)

extract_data >> transform_data >> [load_data, email_op]