"""Подключение необходимых библиотек."""
import json
import os
from datetime import datetime

import numpy as np
import pandas as pd
from airflow import AirflowException
from airflow.providers.postgres.hooks.postgres import PostgresHook


class DataQuality:
    """
    Класс представлен набором процедур проверки и обработки массива данных
    с учетом заданных критериев качества.

    Аттрибуты
    ----------
    data : pd.DataFrame
        проверяемый набор данных
    error_handling:
        параметры проверки качества данных
    data_ref : pd.DataFrame
        набор данных из связанной таблицы
    incorrect_data : pd.DataFrame
        набор данных, не прошедших проверку качества

    Методы
    -------

    """
    def __init__(self, data, error_handling, data_ref=None):
        """
        Конструирует необходимые аттрибуты
        :param data: pd.DataFrame
            проверяемый набор данных
        :param error_handling:
            параметры проверки качества данных
        :param data_ref: pd.DataFrame
            набор данных из связанной таблицы
        """
        self.data = data
        self.error_handling = error_handling
        self.data_ref = data_ref
        self.incorrect_data = pd.DataFrame(columns=[*self.data.columns,
                                                    'noises',
                                                    'missing_values_check',
                                                    'ref_integrity_check',
                                                    'duplicates_check',
                                                    'data_types_check',
                                                    'value_restrict_check',
                                                    'len_restrict_check'])

    @staticmethod
    def data_type_regex(data_type):
        """
        Возвращает регулярное выражение на соответствие указанному типу данных
        :param data_type: str
        :return: str
        """
        if data_type == 'integer':
            return "^\d+$"
        elif data_type == 'numeric':
            return "[+-]?([0-9]*[.])?[0-9]+"
        elif data_type == 'text':
            return "^(?!\s*$).+"
        elif data_type == 'timestamp':
            return "[0-9]{1,2}.[0-9]{1,2}.[0-9]{1,4} [0-9]{1,2}:[0-9]{1,2}"
        elif data_type == 'date_excel':
            return "^-?\d+(\.\d+)?$"

    @staticmethod
    def data_type_convert(data, field, data_type):
        """
        Конвертирует данные указанного поля датафрейма в определенный тип.
        :param data: pd.DataFrame
        :param field: str
        :param data_type: str
        :return: pd.Series
        """
        if data_type == 'integer':
            return pd.to_numeric(data[field], downcast='integer', errors='coerce')
        elif data_type == 'numeric':
            return pd.to_numeric(data[field], errors='coerce')
        elif data_type == 'text':
            return data[field].astype(str)
        elif data_type == 'timestamp':
            return pd.to_datetime(data[field], format='%d.%m.%Y %H:%M', errors='coerce')
        elif data_type == 'date_excel':
            return pd.to_datetime(pd.to_numeric(data[field], errors='coerce'),
                                  unit='D',
                                  origin='1899-12-30')
        elif data_type == 'date':
            return pd.to_datetime(data[field]).dt.date

    @staticmethod
    def multiple_replace(string, phrases):
        """
        Проводит попарную замену символов в строке.
        :param string: str
        :param phrases: dict
        :return: str
        """
        if string:
            for i, j in phrases.items():
                string = string.replace(i, j)
            return string
        return string

    def clear_data(self):
        """
        Заменяет пустые значения и значения, состоящие исключительно из пробелов на Nan-значение
        и устраняет пробелы в начале и конце строки.
        """
        self.data.replace(r'^\s*$', np.nan, regex=True, inplace=True)
        self.data = self.data.apply(lambda x: x.str.strip())

    def noise_restricts_check(self):
        """
        Определяет наличие "шума" в каждом перечисленном поле по указанному регулярному выражению.
        В случае его наличия - устраняет заменой на указанный символ.
        "Зашумленные" данные логгирует.
        """
        noises_data = pd.DataFrame(columns=self.data.columns)
        for field in self.error_handling['noise']:
            regex = self.error_handling['noise'][field]['regex']
            match_replace = self.error_handling['noise'][field]['match_replace']
            mask = self.data[field].str.contains(regex)
            mask.replace(np.nan, False, inplace=True)
            noises_data = pd.concat([noises_data, self.data[mask]])

            if match_replace:
                clean_field = self.data[mask][field].apply((lambda x:
                                                            DataQuality.multiple_replace(x, match_replace)))
                self.data.loc[mask, field] = clean_field

        noises_data.drop_duplicates(inplace=True)
        noises_data["noises"] = True
        self.incorrect_data = pd.concat([self.incorrect_data, noises_data])

    def missing_values_check(self):
        """
        Устраняет из набора данных все строки если пусто
        хотя бы одно поле из критически важного набора полей таблицы.
        В случае, если пусто не критически важное поле - оно заполняется специальным значением.
        Некорректные данные логгирует.
        """
        missing_handling = self.error_handling['missing']
        mask = self.data[missing_handling['drop']].isnull().any(axis=1)

        missing_values = self.data[mask]
        missing_values["missing_values_check"] = True
        self.incorrect_data = pd.concat([self.incorrect_data, missing_values])

        self.data = self.data[~mask]
        if missing_handling['fill']:
            for field in missing_handling['fill']:
                self.data[field].replace('nan', missing_handling['fill'][field], inplace=True)
                self.data.loc[:, field] = self.data[field].fillna(missing_handling['fill'][field])

    def duplicates_check(self):
        """
        Устраняет дублирующиеся по указанному набору полей значения.
        Некорректные данные логгирует.
        """
        for fields in self.error_handling['duplicate']['log']:
            mask = (self.data[fields].duplicated(keep=False))
            duplicates = self.data[mask]
            duplicates["duplicates_check"] = True
            self.incorrect_data = pd.concat([self.incorrect_data, duplicates])
            self.incorrect_data.drop_duplicates(inplace=True)

        self.data.drop_duplicates(subset=self.error_handling['duplicate']['drop'], inplace=True)

    def data_types_check(self):
        """
        На основе регулярного выражения определяет тип данных в полях,
        и затем значения конвертируются.
        Некорректные данные логгирует.
        """
        mask = pd.DataFrame(columns=self.data.columns)
        for field in self.error_handling['data_types']:
            regex = DataQuality.data_type_regex(self.error_handling['data_types'][field])
            mask[field] = self.data[field].str.match(regex)
        mask.replace(np.nan, False, inplace=True)

        data_types_invalid = self.data[~mask]
        data_types_invalid.dropna(inplace=True)
        data_types_invalid["data_types_check"] = True
        self.incorrect_data = pd.concat([self.incorrect_data, data_types_invalid])

        for field in self.error_handling['data_types']:
            self.data.loc[:, field] = DataQuality.data_type_convert(self.data,
                                                                    field,
                                                                    self.error_handling['data_types'][field])

    def ref_integrity(self):
        """
        Устраняет из проверяемой таблицы те строки по указанному полю,
        которые отсутствуют в связанном наборе данных.
        """

        ref_integrity = self.error_handling['ref_integrity']
        for table_ref in ref_integrity:
            data_union = self.data_ref[table_ref].merge(self.data,
                                                        on=ref_integrity[table_ref]['fields'],
                                                        how='outer',
                                                        indicator=True)

            self.data = data_union[data_union['_merge'] == 'both'].drop('_merge', axis=1)

            ref_integrity_invalid = data_union[data_union['_merge'] == 'right_only'].drop('_merge', axis=1)
            ref_integrity_invalid["ref_integrity_check"] = True
            self.incorrect_data = pd.concat([self.incorrect_data, ref_integrity_invalid])

    def value_restrict_check(self):
        """
        Проверяет значения на соответствие указанному критерию.
        Некорректные данные устраняются и логгируются.
        """
        for field in self.error_handling['val_restrict']:
            query = self.error_handling['val_restrict'][field]
            field_data = DataQuality.data_type_convert(self.data, field, self.error_handling['data_types'][field])
            mask = field_data.to_frame().eval(f'{field} != {field} or {query}')

            value_invalid = self.data[~mask]
            value_invalid["value_restrict_check"] = True
            self.incorrect_data = pd.concat([self.incorrect_data, value_invalid])

            self.data = self.data[mask]

    def len_restricts_check(self):
        """
        Проверяет соответствие длины строки крайним значениям.
        Подозрительные данные логгируются.
        """
        for field in self.error_handling['len_restrict']:
            min_len = self.error_handling['len_restrict'][field]['min']
            max_len = self.error_handling['len_restrict'][field]['max']
            if not min_len:
                query = f"({field}.str.len() > {max_len})"
            elif not max_len:
                query = f"({field}.str.len() < {min_len})"
            else:
                query = f"({field}.str.len() < {min_len} or {field}.str.len() > {max_len})"
            field_data = self.data[field]
            mask = field_data.to_frame().eval(query)

            len_invalid = self.data[mask]
            len_invalid["len_restrict_check"] = True
            self.incorrect_data = pd.concat([self.incorrect_data, len_invalid])

    def check(self):
        """
        Выполняет все процедуры класса.
        """
        self.clear_data()
        if self.error_handling['noise']:
            self.noise_restricts_check()
        self.data_types_check()
        self.missing_values_check()
        self.duplicates_check()
        if self.error_handling['val_restrict']:
            self.value_restrict_check()
        if self.error_handling['len_restrict']:
            self.len_restricts_check()
        if self.error_handling['ref_integrity']:
            self.ref_integrity()


def data_quality(table,
                 conn_info,
                 hook_from,
                 hook_to,
                 logs,
                 error_handling):
    """
    Запускает проверку качества данных и сохраняет обработанный набор данных.
    Перед запуском проверки получает данные связанных таблиц.
    :param conn_info: конфигурационные данные соединений.
    :param hook_from: хук соединения исходной базы данных
    :param hook_to: хук соединения целевой базы данных.
    :param table: наименование проверяемой таблицы.
    :param logs: параметры логгирования.
    :param error_handling: параметры проверки качества данных.
    """
    request_data = f"select * from {conn_info['tables'][table]['from']['schema']}.{table};"
    data_raw = hook_from.get_pandas_df(sql=request_data)

    refs = dict()
    if error_handling['ref_integrity']:
        for table_ref in error_handling['ref_integrity']:
            conn_id = conn_info['tables'][table_ref]['to']['conn_id']
            schema = conn_info['tables'][table_ref]['to']['schema']
            fields_ref = error_handling['ref_integrity'][table_ref]['fields_ref']
            try:
                hook = PostgresHook(postgres_conn_id=conn_id)
                print(f"Postgres connects success to the {table_ref} table")
            except Exception as error:
                raise AirflowException(f"ERROR: Connect error with table {table_ref}: {error}") from error
            request_ref_data = f"select {', '.join(fields_ref)} from {schema}.{table_ref};"
            data_ref = hook.get_pandas_df(sql=request_ref_data)
            refs[table_ref] = data_ref

    data_checked = DataQuality(data=data_raw, data_ref=refs, error_handling=error_handling)
    data_checked.check()

    data_checked.data.to_sql(name=table,
                             schema=conn_info['tables'][table]['to']['schema'],
                             con=hook_to.get_sqlalchemy_engine(),
                             if_exists='append',
                             index=False)
    data_checked.incorrect_data.to_sql(name=logs['table_log'],
                                       schema=logs['schema'],
                                       con=hook_to.get_sqlalchemy_engine(),
                                       if_exists='append',
                                       index=False)
    hook_to.get_conn().commit()

    upload_info = pd.DataFrame(data={'table_name': [table],
                                     'update_date': [datetime.now()],
                                     'load_id': [round(datetime.now().timestamp())]})
    upload_info.to_sql(name='upload_tables_tech',
                       schema=logs['schema'],
                       con=hook_to.get_sqlalchemy_engine(),
                       if_exists='append',
                       index=False)
    hook_to.get_conn().commit()


dq_params_path = os.path.expanduser("/opt/airflow/scripts/data_quality_params.json")

# Параметры подключения и проверки качества данных
with open(dq_params_path, 'r', encoding="utf-8") as json_file:
    dq_params = json.load(json_file)

# Формирование запроса на удаление всех данных из всех таблиц
remove_all_data = ''
for table in dq_params['conn_info']['tables']:
    table_schema = dq_params['conn_info']['tables'][table]['to']['schema']
    table_log = dq_params['log_info']['tables'][table]['table_log']
    table_log_schema = dq_params['log_info']['tables'][table]['schema']
    remove_all_data += f"TRUNCATE {table_schema}.{table} CASCADE;\n"
    remove_all_data += f"TRUNCATE {table_log_schema}.{table_log} CASCADE;\n"
