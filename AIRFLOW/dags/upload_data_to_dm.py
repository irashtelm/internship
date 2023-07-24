"""Подключение необходимых библиотек."""
from datetime import datetime

from airflow import AirflowException
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
import data_marts as dm


def data_upload_dag(data_marts_params, mart):
    try:
        hook_from = PostgresHook(postgres_conn_id=data_marts_params['conn_info']['from']['conn_id'])
        hook_to = PostgresHook(postgres_conn_id=data_marts_params['conn_info']['to']['conn_id'])
        print(f"Postgres connects success")
    except Exception as error:
        raise AirflowException(f"ERROR: Connect error: {error}") from error

    try:
        dm.calculate_statistics(data_marts_params=data_marts_params,
                             mart=mart,
                             hook_from=hook_from,
                             hook_to=hook_to)
    except Exception as error:
        raise AirflowException(f"ERROR of execute formation_orders_data function: {error}") from error


with DAG('data_upload_to_dm', description="",
         schedule_interval="@once",
         start_date=datetime(2023, 7, 24),
         catchup=False,
         tags=["etl-process"]) as dag:

    remove_all_dm_data = PostgresOperator(task_id='remove_all_dm_data',
                                          sql=dm.remove_all_data,
                                          postgres_conn_id=dm.dm_params['conn_info']['to']['conn_id'])

    orders_data_upload = PythonOperator(task_id="orders_data_upload",
                                        trigger_rule="all_success",
                                        python_callable=data_upload_dag,
                                        op_kwargs={
                                            'data_marts_params': dm.dm_params,
                                            'mart': 'orders_data'
                                        })

    stock_data_upload = PythonOperator(task_id="stock_data_upload",
                                       trigger_rule="all_success",
                                       python_callable=data_upload_dag,
                                       op_kwargs={
                                           'data_marts_params': dm.dm_params,
                                           'mart': 'stock_data'
                                       })

    create_orders_view = PostgresOperator(task_id='create_orders_view',
                                          sql=dm.create_view_queries['orders_data'],
                                          postgres_conn_id=dm.dm_params['conn_info']['to']['conn_id'])

    create_stock_view = PostgresOperator(task_id='create_stock_view',
                                         sql=dm.create_view_queries['stock_data'],
                                         postgres_conn_id=dm.dm_params['conn_info']['to']['conn_id'])

    remove_all_dm_data >> [orders_data_upload, stock_data_upload ]
    orders_data_upload >> create_orders_view
    stock_data_upload >> create_stock_view
