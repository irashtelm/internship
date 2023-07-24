"""Подключение необходимых библиотек."""
import json
import os
from datetime import datetime

from data_quality import DataQuality as Dq


def join_tables(join_info, schema, hook):
    """
    Объединяет несколько наборов данных через операцию join.
    :param join_info: dict информация о соединяемых таблицах
    :param schema: str схема целевой базы данных
    :param hook: хук соединения исходной базы данных
    :return: объединенный набор данных
    """
    request_data = f"select * from {schema}.{join_info['source']};"
    data = hook.get_pandas_df(sql=request_data)
    data.rename(columns={col: f"{join_info['source']}_{col}" for col in data.columns},
                inplace=True)

    for joined_table in join_info['joined_tables']:
        request_joined_data = f"select * from {schema}.{joined_table['table']};"
        joined_data = hook.get_pandas_df(sql=request_joined_data)

        joined_data.rename(columns={col: f"{joined_table['table']}_{col}" for col in joined_data.columns},
                           inplace=True)
        if joined_table["cast"]:
            for field in joined_table["cast"]:
                data[f"{field}_cast"] = Dq.data_type_convert(data=data,
                                                             field=field,
                                                             data_type=joined_table["cast"][field])

        data = data.merge(joined_data,
                          how=joined_table['how'],
                          left_on=joined_table['left_on'],
                          right_on=joined_table['right_on'])
    return data


def formation_data(calculate_new_col):
    def wrapper(data_marts_params, mart, hook_from, hook_to):
        conn_info = data_marts_params['conn_info']
        join_info = data_marts_params['marts'][mart]['join_info']
        data_joined = join_tables(join_info=join_info,
                                  schema=conn_info['from']['schema'],
                                  hook=hook_from)

        calculate_new_col(mart=mart, data=data_joined)

        data_joined["load_id"] = round(datetime.now().timestamp())
        data_joined.to_sql(name=mart,
                           schema=conn_info['to']['schema'],
                           con=hook_to.get_sqlalchemy_engine(),
                           if_exists='append',
                           index=False)
    return wrapper


@formation_data
def calculate_statistics(mart, data):
    if mart == 'orders_data':
        data['order_amount'] = data['transaction_quantity'] * data['transaction_price']
        data['order_amount_full_price'] = data['transaction_quantity'] * data['transaction_price_full']
    elif mart == 'stock_data':
        data['available_amount'] = data['stock_available_quantity'] * data['stock_cost_per_item']


dm_params_path = os.path.expanduser("/opt/airflow/scripts/data_marts_params.json")

# Параметры подключения и проверки качества данных
with open(dm_params_path, 'r', encoding="utf-8") as json_file:
    dm_params = json.load(json_file)


remove_all_data = ''
for table in dm_params['marts']:
    table_schema = dm_params['conn_info']['to']['schema']
    remove_all_data += f"TRUNCATE {table_schema}.{table} CASCADE;\n"


create_view_queries = dict()
for mart in dm_params['marts']:
    schema = dm_params['conn_info']['to']['schema']
    columns = dm_params['marts'][mart]['column_names']
    view_columns = ',\n'.join([f"{key} as \"{val}\"" for key, val in columns.items()])
    create_view_query = f"""
    create or replace view {schema}.{mart}_mart as
    select {view_columns}
    from {schema}.{mart}
    """
    create_view_queries[mart] = create_view_query
