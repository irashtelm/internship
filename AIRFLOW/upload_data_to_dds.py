"""Подключение необходимых библиотек."""
from datetime import datetime

import data_quality as dq
from airflow import AirflowException
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator


def data_upload_dag(conn_from,
                    table,
                    logs,
                    conn_to,
                    error_handling):
    try:
        hook_schema_from = {'hook': PostgresHook(postgres_conn_id=conn_from['conn_id']), 'schema': conn_from['schema']}
        hook_schema_to = {'hook': PostgresHook(postgres_conn_id=conn_to['conn_id']), 'schema': conn_to['schema']}
        print("Postgres connect success")
    except Exception as error:
        raise AirflowException(f"ERROR: Connect error: {error}") from error
    try:
        dq.data_quality(conn_from=hook_schema_from,
                        table=table,
                        logs=logs,
                        conn_to=hook_schema_to,
                        error_handling=error_handling)
    except Exception as error:
        raise AirflowException(f"ERROR: {error}") from error
    

with DAG('data_upload', description="",
         schedule_interval="@once",  # @daily
         start_date=datetime(2023, 7, 16),
         catchup=False,  # ???
         tags=["learning"]) as dag:
    start_step = EmptyOperator(task_id="start_step")

    layer_recreation = PostgresOperator(task_id='layer_recreation',
                                        sql=dq.layer_recreation_query,
                                        postgres_conn_id=dq.conn_info_to['conn_id']
                                        )

    brand_upload = PythonOperator(task_id="brand_upload",
                                  trigger_rule="all_success",
                                  python_callable=data_upload_dag,
                                  op_kwargs={
                                      'conn_from': dq.conn_info['brand']['from'],
                                      'table': 'brand',
                                      'logs': dq.tables_log['brand'],
                                      'conn_to': dq.conn_info['brand']['to'],
                                      'error_handling': dq.error_handling['brand']
                                  }
                                  )

    category_upload = PythonOperator(task_id="category_upload",
                                     trigger_rule="all_success",
                                     python_callable=data_upload_dag,
                                     op_kwargs={
                                         'conn_from': dq.conn_info['category']['from'],
                                         'table': 'category',
                                         'logs': dq.tables_log['category'],
                                         'conn_to': dq.conn_info['category']['to'],
                                         'error_handling': dq.error_handling['category']
                                     }
                                     )

    stores_upload = PythonOperator(task_id="stores_upload",
                                   trigger_rule="all_success",
                                   python_callable=data_upload_dag,
                                   op_kwargs={
                                       'conn_from': dq.conn_info['stores']['from'],
                                       'table': 'stores',
                                       'logs': dq.tables_log['stores'],
                                       'conn_to': dq.conn_info['stores']['to'],
                                       'error_handling': dq.error_handling['stores']
                                   }
                                   )

    product_upload = PythonOperator(task_id="product_upload",
                                    trigger_rule="all_success",
                                    python_callable=data_upload_dag,
                                    op_kwargs={
                                        'conn_from': dq.conn_info['product']['from'],
                                        'table': 'product',
                                        'logs': dq.tables_log['product'],
                                        'conn_to': dq.conn_info['product']['to'],
                                        'error_handling': dq.error_handling['product']
                                    }
                                    )

    transaction_stores_upload = PythonOperator(task_id="transaction_stores_upload",
                                               trigger_rule="all_success",
                                               python_callable=data_upload_dag,
                                               op_kwargs={
                                                   'conn_from': dq.conn_info['transaction_stores']['from'],
                                                   'table': 'transaction_stores',
                                                   'logs': dq.tables_log['transaction_stores'],
                                                   'conn_to': dq.conn_info['transaction_stores']['to'],
                                                   'error_handling': dq.error_handling['transaction_stores']
                                               }
                                               )

    stock_upload = PythonOperator(task_id="stock_upload",
                                  trigger_rule="all_success",
                                  python_callable=data_upload_dag,
                                  op_kwargs={
                                      'conn_from': dq.conn_info['stock']['from'],
                                      'table': 'stock',
                                      'logs': dq.tables_log['stock'],
                                      'conn_to': dq.conn_info['stock']['to'],
                                      'error_handling': dq.error_handling['stock']
                                  }
                                  )

    transaction_upload = PythonOperator(task_id="transaction_upload",
                                        trigger_rule="all_success",
                                        python_callable=data_upload_dag,
                                        op_kwargs={
                                            'conn_from': dq.conn_info['transaction']['from'],
                                            'table': 'transaction',
                                            'logs': dq.tables_log['transaction'],
                                            'conn_to': dq.conn_info['transaction']['to'],
                                            'error_handling': dq.error_handling['transaction']
                                        }
                                        )

    end_step = EmptyOperator(task_id="end_step",
                             trigger_rule="all_success")

    start_step >> layer_recreation
    layer_recreation >> [brand_upload, category_upload, stores_upload]
    [brand_upload, category_upload] >> product_upload
    stores_upload >> [transaction_stores_upload, stock_upload]
    product_upload >> [stock_upload, transaction_upload]
    transaction_stores_upload >> transaction_upload
    [stock_upload, transaction_upload] >> end_step
