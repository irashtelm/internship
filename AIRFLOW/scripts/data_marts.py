"""Подключение необходимых библиотек."""
import os
import smtplib
from datetime import datetime
from decimal import Decimal
from email import encoders
from email.header import Header
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from json import load
from typing import Callable, Union, List, Dict

import pandas as pd
from airflow import AirflowException
from airflow.providers.postgres.hooks.postgres import PostgresHook
from data_quality import DataQuality as Dq


def formation_data(customization_func: Callable) -> Callable:
    """
    Декорирует исполнение вложенной функции получением исходных
    данных и последующей загрузкой данных,
    к которым была применена вложенная функция, в целевую таблицу.
    :param customization_func: функция, изменяющая набор исходных данных.
    :return: функция-обертка
    """
    def wrapper(self, *args, **kwargs):
        schema = self.data_marts_params['conn_info']['to']['schema']
        self.get_data()

        customization_func(self, *args, **kwargs)

        self.data["load_id"] = round(datetime.now().timestamp())
        self.data.to_sql(name=self.mart,
                         schema=schema,
                         con=self.hook_to.get_sqlalchemy_engine(),
                         if_exists='append',
                         index=False)
    return wrapper


class CreateMarts:
    """
    Класс представлен набором процедур формирования конкретных таблиц данных,
    на основе которых формируются витрины данных.

    Аттрибуты
    ----------
    mart: наименование целевой таблицы
    data: формируемый набор данных
    data_marts_params: параметры формирования набора данных
    hook_from: хук соединения исходной базы данных
    hook_to: хук соединения целевой базы данных
    email_info: информация об отправителе писем

    Методы
    -------
    rename_columns: переименование полей с указанием префикса
    send_message: отправка сообщения на почту
    get_data: получение данных из исходной таблицы и присоединение
    к ней наборов данных других таблиц
    create_mart: формирование набора данных и его загрузка в целевую таблицу
    orders_data: формирование таблицы orders_data
    stock_data: формирование таблицы stock_data
    stores_data: формирование таблицы stores_data
    """
    def __init__(self,
                 mart: str,
                 data_marts_params: Union[List[Dict], Dict[str, Dict]],
                 hook_from: PostgresHook,
                 hook_to: PostgresHook,
                 email_info: dict = None):
        """
        Конструирует необходимые аттрибуты.
        :param mart: наименование таблицы данных-основы для создания витрины
        :param data_marts_params: параметры формирования набора данных
        :param hook_from: хук соединения исходной базы данных
        :param hook_to: хук соединения целевой базы данных
        """
        self.mart = mart
        self.data = None
        self.data_marts_params = data_marts_params
        self.hook_from = hook_from
        self.hook_to = hook_to
        self.email_info = email_info

    @staticmethod
    def rename_columns(data: pd.DataFrame, prefix: str) -> None:
        """
        Переименовывает поля набора данных добавлением указанного префикса.
        :param data: набор обрабатываемых данных.
        :param prefix: префикс переименования.
        """
        data.rename(columns={col: f"{prefix}_{col}" for col in data.columns}, inplace=True)

    @staticmethod
    def send_message(subject: str, body: pd.DataFrame, sender: str,
                     password: str, recipient: str) -> None:
        """
        Отправляет почтовое сообщение
        :param subject: тема
        :param body: содержание
        :param sender: отправитель
        :param password: пароль
        :param recipient: получатель
        """

        # конвертирование набора данных в файл формата .csv
        csv_data = body.to_csv(index=False, encoding="unicode_escape")

        # метаданные письма
        message = MIMEMultipart()
        message["From"] = sender
        message["To"] = recipient
        message["Subject"] = subject

        # прикрепление .csv файла к письму
        attachment = MIMEBase('application', 'octet-stream')
        attachment.set_payload(csv_data.encode('utf-8'))
        encoders.encode_base64(attachment)
        attachment.add_header('Content-Disposition',
                              'attachment',
                              filename=Header(f'{subject}.csv', 'utf-8').encode())
        message.attach(attachment)

        # отправка письма
        try:
            with smtplib.SMTP("smtp.gmail.com", 587) as server:
                server.starttls()
                server.login(sender, password)
                server.sendmail(sender, recipient, message.as_string())
            print("Email sent successfully!")
        except Exception as error:
            raise AirflowException(f"Error sending email: {error}") from error

    def get_data(self):
        """
        Формирует набор данных целевой таблицы получением и
        соединением наборов данных из указанных таблиц
        с учетом указанных параметров соединения таблиц.
        """
        join_info = self.data_marts_params['join_info']
        schema = self.data_marts_params['conn_info']['from']['schema']
        hook = self.hook_from

        # получение исходной таблицы
        request_data = f"select * from {schema}.{join_info['source']['table']};"
        data = hook.get_pandas_df(sql=request_data)
        if join_info['source']['rename']:
            CreateMarts.rename_columns(data, join_info['source']['table'])

        # последовательное присоединение указанных таблиц к исходной
        if join_info['joined_tables']:
            for joined_table in join_info['joined_tables']:
                # получение данных присоединяемой таблицы
                request_joined_data = f"select * from {schema}.{joined_table['table']};"
                joined_data = hook.get_pandas_df(sql=request_joined_data)

                if joined_table['rename']:
                    CreateMarts.rename_columns(joined_data, joined_table['table'])

                # создание дополнительного поля, являющегося частью
                # ключа соединения, конвертированием типа данных
                if joined_table["cast"]:
                    for field in joined_table["cast"]:
                        data[f"{field}_cast"] = Dq.\
                            data_type_convert(data=data,
                                              field=field,
                                              data_type=joined_table["cast"][field])

                # объединение таблиц
                data = data.merge(joined_data,
                                  how=joined_table['how'],
                                  left_on=joined_table['left_on'],
                                  right_on=joined_table['right_on'])
        # присвоение полученного объекта
        self.data = data

    def create_mart(self):
        """
        Вызывает функцию, формирующую указанную таблицу
        """
        getattr(self, self.mart)()

    @formation_data
    def orders_data(self):
        """
        Формирует таблицу orders_data добавлением двух полей
        с расчетом сумм заказа до и после скидки.
        """
        self.data['order_amount'] = self.data['transaction_quantity']\
            .astype(str).apply(Decimal).mul(self.data['transaction_price']
                                            .astype(str).apply(Decimal))
        self.data['order_amount_full_price'] = self.data['transaction_quantity']\
            .astype(str).apply(Decimal).mul(self.data['transaction_price_full']
                                            .astype(str).apply(Decimal))

    @formation_data
    def stock_data(self):
        """
        Формирует таблицу stock_data добавлением поля
        с расчетом стоимости доступного количества товара.
        """
        self.data['available_amount'] = self.data['stock_available_quantity']\
            .astype(str).apply(Decimal).mul(self.data['stock_cost_per_item']
                                            .astype(str).apply(Decimal))

    @formation_data
    def stores_data(self):
        """
        Формирует таблицу stores_data с информацией
        о критически низком количестве товара по каждому магазину
        и отправляет эти сведения им на почту.
        """
        cols = self.data_marts_params['column_names'].keys()
        cols_drop = [col for col in self.data.columns if col not in cols]
        self.data.drop(columns=cols_drop, inplace=True)
        query = "stock_available_quantity < product_quantity_min_quantity"
        self.data.query(query, inplace=True)

        # отправка данных только за последнюю дату
        data = self.data.query("stock_available_on == stock_available_on.max()")
        # отправка данных по каждому магазину
        for email in data['stores_emails_email'].unique():
            data_send = data[data['stores_emails_email'] == email]
            # переименование полей отправляемой таблицы и их выборка
            print(f"there {email}")
            print(f"there-1 {data_send.columns}")
            data_send = data_send[self.data_marts_params['column_names'].keys()]\
                .rename(columns=self.data_marts_params['column_names'])
            print(f"there-2 {data_send.columns}")
            data_send = data_send[['Магазин', 'ID товара', 'Товар', 'Дата наличия товара',
                                   'Доступное количество товара, шт.', 'Минимальное количество товара, шт.']]
            CreateMarts.send_message(subject="Товары с остатком ниже минимально допустимого",
                                     body=data_send,
                                     sender=self.email_info['sender'],
                                     password=self.email_info['password'],
                                     recipient=email)


dm_params_path = os.path.expanduser("/opt/airflow/scripts/data_marts_params.json")


# Параметры подключения и проверки качества данных
with open(dm_params_path, 'r', encoding="utf-8") as json_file:
    dm_params = load(json_file)

remove_all_data = ''
for dm_mart in dm_params['marts']:
    table_schema = dm_params['marts'][dm_mart]['conn_info']['to']['schema']
    remove_all_data += f"TRUNCATE {table_schema}.{dm_mart} CASCADE;\n"

create_view_queries = {}
for dm_mart in dm_params['marts']:
    mart_schema = dm_params['marts'][dm_mart]['conn_info']['to']['schema']
    columns = dm_params['marts'][dm_mart]['column_names']
    view_columns = ',\n'.join([f"{key} as \"{val}\"" for key, val in columns.items()])
    create_view_query = f"""
    create or replace view {mart_schema}.{dm_mart}_mart as
    select {view_columns}
    from {mart_schema}.{dm_mart}
    """
    create_view_queries[dm_mart] = create_view_query
