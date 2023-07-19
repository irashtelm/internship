"""Подключение необходимых библиотек."""
from datetime import datetime

from airflow import AirflowException
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


def run_select_n_rows_query(table: str, conn, n_rows: int):
    """
    Выводит указанное количество первых строк выбранной таблицы
    :param table: наименование таблицы
    :param conn: конфигурационные данные соединения
    :param n_rows: количество выводимых строк
    """
    cursor = conn.cursor()

    select = f"SELECT * FROM {table}"
    cursor.execute(select)
    records = cursor.fetchmany(n_rows)
    conn.commit()
    print(f"Fetching {n_rows} rows")
    for row in records:
        print(row)

    cursor.close()
    conn.close()


def select_data(table: str, conn_id, n_rows: int=10):
    """
    В случае успешного подключения выполняет вложенную функцию с запросом к базе данных
    :param table: наименование таблицы
    :param conn: конфигурационные данные соединения
    :param n_rows: количество выводимых строк
    """
    try:
        pg_hook = PostgresHook(postgres_conn_id=conn_id)
        conn = pg_hook.get_conn()
        print("Postgress connect success")
    except Exception as error:
        raise AirflowException(f"ERROR: Connect error: {error}") from error
    try:
        run_select_n_rows_query(table, conn, n_rows)
    except Exception as error:
        raise AirflowException(f"ERROR: Connect error: {error}") from error


config = Variable.get("select_n_rows", deserialize_json=True)

with DAG('select_data', description="Select some data from source",
         schedule_interval="@once",
         start_date=datetime(2023, 7, 12),
         catchup=False,
         tags=["learning"]) as dag:

    start_step = EmptyOperator(task_id="start_step")

    select_data = PythonOperator(task_id="select_data",
                                 python_callable=select_data,
                                 op_args=[f"{config['schema']}.{config['table']}",
                                          "sources_id",
                                          int(f"{config['rows_number']}")])

    end_step = EmptyOperator(task_id="end_step")

    start_step >> select_data >> end_step
