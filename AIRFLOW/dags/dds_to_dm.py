"""Подключение необходимых библиотек."""
from datetime import datetime
from typing import Union, List, Dict

import data_marts as dm
from airflow import AirflowException, DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models import Variable


email = Variable.get("email_info", deserialize_json=True)


def data_upload_dag(data_marts_params: Union[List[Dict], Dict[str, Dict]],
                    mart: str,
                    email_info: dict = None) -> None:
    """
    Считывает конфигурации и параметры подключения к БД
    и запускает процедуры формирования витрин данных.
    :param data_marts_params: параметры формирования набора данных.
    :param mart: наименование целевой таблицы.
    :param email_info: информация об отправителе писем.
    """
    try:
        hook_from = PostgresHook(postgres_conn_id=data_marts_params['conn_info']['from']['conn_id'])
        hook_to = PostgresHook(postgres_conn_id=data_marts_params['conn_info']['to']['conn_id'])
        print("Postgres connects success")
    except Exception as error:
        raise AirflowException(f"ERROR: Connect error: {error}") from error

    try:
        dm.CreateMarts(data_marts_params=data_marts_params,
                       mart=mart,
                       hook_from=hook_from,
                       hook_to=hook_to,
                       email_info=email_info).create_mart()
    except Exception as error:
        raise AirflowException(f"ERROR: {error}") from error


with DAG('dds_to_dm',
         description="Загрузка данных со слоя dds на слой datamarts",
         # расписание запуска: окончание выполнение DAG'а "sources_to_dds" в течение 5 минут
         schedule_interval="5 0 * * *",
         start_date=datetime(2023, 7, 1),
         catchup=False,
         tags=["etl-process"]) as dag:

    remove_all_dm_data = PostgresOperator(task_id=
                                          'remove_all_dm_data',
                                          sql=
                                          dm.remove_all_data,
                                          postgres_conn_id=
                                          dm.dm_params['conn_info']['to']['conn_id'])

    orders_data_upload = PythonOperator(task_id="orders_data_upload",
                                        trigger_rule="all_success",
                                        python_callable=data_upload_dag,
                                        op_kwargs={
                                            'data_marts_params':
                                                dm.dm_params['marts']['orders_data'],
                                            'mart': 'orders_data'
                                        })

    stock_data_upload = PythonOperator(task_id="stock_data_upload",
                                       trigger_rule="all_success",
                                       python_callable=data_upload_dag,
                                       op_kwargs={
                                           'data_marts_params': dm.dm_params['marts']['stock_data'],
                                           'mart': 'stock_data'
                                       })

    stores_data_upload = PythonOperator(task_id="stores_data_upload",
                                        trigger_rule="all_success",
                                        python_callable=data_upload_dag,
                                        op_kwargs={
                                            'data_marts_params':
                                                dm.dm_params['marts']['stores_data'],
                                            'mart': 'stores_data',
                                            'email_info': email
                                        })

    create_orders_view = PostgresOperator(task_id='create_orders_view',
                                          sql=dm.create_view_queries['orders_data'],
                                          postgres_conn_id=dm.dm_params['marts']['orders_data']
                                          ['conn_info']['to']['conn_id'])

    create_stock_view = PostgresOperator(task_id='create_stock_view',
                                         sql=dm.create_view_queries['stock_data'],
                                         postgres_conn_id=dm.dm_params['marts']['stock_data']
                                         ['conn_info']['to']['conn_id'])

    create_stores_view = PostgresOperator(task_id='create_stores_view',
                                          sql=dm.create_view_queries['stores_data'],
                                          postgres_conn_id=dm.dm_params['marts']['stores_data']
                                          ['conn_info']['to']['conn_id'])

    remove_all_dm_data >> [orders_data_upload, stock_data_upload]
    orders_data_upload >> create_orders_view
    stock_data_upload >> [stores_data_upload, create_stock_view]
    stores_data_upload >> create_stores_view
