"""Подключение необходимых библиотек."""
from datetime import datetime
from typing import Union, List, Dict

import data_quality as dq
from airflow import AirflowException, DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator


def data_upload_dag(table: str,
                    conn_info: Union[List[Dict], Dict[str, Dict]],
                    logs: dict,
                    error_handling: Dict[str, Dict]) -> None:
    """
    Считывает конфигурации и параметры подключения к БД
    и запускает проверку качества данных таблицы.
    :param table: наименование таблицы.
    :param conn_info: конфигурационные сведения.
    :param logs: параметры журнала событий.
    :param error_handling: параметры проверки качества данных.
    """
    try:
        hook_from = PostgresHook(postgres_conn_id=conn_info['tables'][table]['from']['conn_id'])
        hook_to = PostgresHook(postgres_conn_id=conn_info['tables'][table]['to']['conn_id'])
        print("Postgres connect success")
    except Exception as error:
        raise AirflowException(f"ERROR: Connect error: {error}") from error
    try:
        dq.data_quality(table=table,
                        conn_info=conn_info,
                        hook_from=hook_from,
                        hook_to=hook_to,
                        logs=logs,
                        error_handling=error_handling)
    except Exception as error:
        raise AirflowException(f"ERROR: {error}") from error


with DAG('sources_to_dds',
         description="Загрузка данных со слоя sources на слой dds",
         schedule_interval="0 0 * * *",
         start_date=datetime(2023, 7, 1),
         catchup=False,
         tags=["etl-process"]) as dag:

    remove_all_data = PostgresOperator(task_id='remove_all_data',
                                       sql=dq.remove_all_data,
                                       postgres_conn_id=dq.dq_params['conn_info']['to']['conn_id']
                                       )

    brand_upload = PythonOperator(task_id="brand_upload",
                                  trigger_rule="all_success",
                                  python_callable=data_upload_dag,
                                  op_kwargs={
                                      'table': 'brand',
                                      'conn_info': dq.dq_params['conn_info'],
                                      'logs': dq.dq_params['log_info']['tables']['brand'],
                                      'error_handling': dq.dq_params['error_handling']['brand']
                                  }
                                  )

    category_upload = PythonOperator(task_id="category_upload",
                                     trigger_rule="all_success",
                                     python_callable=data_upload_dag,
                                     op_kwargs={
                                         'table': 'category',
                                         'conn_info':
                                             dq.dq_params['conn_info'],
                                         'logs':
                                             dq.dq_params['log_info']['tables']['category'],
                                         'error_handling':
                                             dq.dq_params['error_handling']['category']
                                     }
                                     )

    stores_upload = PythonOperator(task_id="stores_upload",
                                   trigger_rule="all_success",
                                   python_callable=data_upload_dag,
                                   op_kwargs={
                                       'table': 'stores',
                                       'conn_info': dq.dq_params['conn_info'],
                                       'logs': dq.dq_params['log_info']['tables']['stores'],
                                       'error_handling': dq.dq_params['error_handling']['stores']
                                   }
                                   )

    stores_emails_upload = PythonOperator(task_id="stores_emails_upload",
                                          trigger_rule="all_success",
                                          python_callable=data_upload_dag,
                                          op_kwargs={
                                              'table':
                                                  'stores_emails',
                                              'conn_info':
                                                  dq.dq_params['conn_info'],
                                              'logs':
                                                  dq.dq_params['log_info']['tables']['stores_emails'],
                                              'error_handling':
                                                  dq.dq_params['error_handling']['stores_emails']
                                          }
                                          )

    product_upload = PythonOperator(task_id="product_upload",
                                    trigger_rule="all_success",
                                    python_callable=data_upload_dag,
                                    op_kwargs={
                                        'table': 'product',
                                        'conn_info': dq.dq_params['conn_info'],
                                        'logs': dq.dq_params['log_info']['tables']['product'],
                                        'error_handling': dq.dq_params['error_handling']['product']
                                    }
                                    )

    product_quantity_upload = PythonOperator(task_id="product_quantity_upload",
                                             trigger_rule="all_success",
                                             python_callable=data_upload_dag,
                                             op_kwargs={
                                                 'table':
                                                     'product_quantity',
                                                 'conn_info':
                                                     dq.dq_params['conn_info'],
                                                 'logs':
                                                     dq.dq_params['log_info']['tables']['product_quantity'],
                                                 'error_handling':
                                                     dq.dq_params['error_handling']['product_quantity']
                                             }
                                             )

    transaction_stores_upload = PythonOperator(task_id="transaction_stores_upload",
                                               trigger_rule="all_success",
                                               python_callable=data_upload_dag,
                                               op_kwargs={
                                                   'table':
                                                       'transaction_stores',
                                                   'conn_info':
                                                       dq.dq_params['conn_info'],
                                                   'logs':
                                                       dq.dq_params['log_info']['tables']['transaction_stores'],
                                                   'error_handling':
                                                       dq.dq_params['error_handling']['transaction_stores']
                                               }
                                               )

    stock_upload = PythonOperator(task_id="stock_upload",
                                  trigger_rule="all_success",
                                  python_callable=data_upload_dag,
                                  op_kwargs={
                                      'table': 'stock',
                                      'conn_info': dq.dq_params['conn_info'],
                                      'logs': dq.dq_params['log_info']['tables']['stock'],
                                      'error_handling': dq.dq_params['error_handling']['stock']
                                  }
                                  )

    transaction_upload = PythonOperator(task_id="transaction_upload",
                                        trigger_rule="all_success",
                                        python_callable=data_upload_dag,
                                        op_kwargs={
                                            'table':
                                                'transaction',
                                            'conn_info':
                                                dq.dq_params['conn_info'],
                                            'logs':
                                                dq.dq_params['log_info']['tables']['transaction'],
                                            'error_handling':
                                                dq.dq_params['error_handling']['transaction']
                                        }
                                        )

    remove_all_data >> [brand_upload, category_upload, stores_upload]
    [brand_upload, category_upload] >> product_upload
    stores_upload >> [transaction_stores_upload, stock_upload, stores_emails_upload]
    product_upload >> [product_quantity_upload, stock_upload, transaction_upload]
    transaction_stores_upload >> transaction_upload
