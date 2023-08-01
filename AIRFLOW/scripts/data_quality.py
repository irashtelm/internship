"""Подключение необходимых библиотек."""
from datetime import datetime
from json import load
from os.path import expanduser
from typing import Union, List, Dict

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
    data: проверяемый набор данных
    error_handling: параметры проверки качества данных
    data_ref: набор данных из связанной таблицы
    incorrect_data: набор данных, не прошедших проверку качества

    Методы
    -------
    data_type_regex: сопоставляет регулярное выражение и тип данных
    data_type_convert: конвертирует поле набора данных в указанный тип данных
    multiple_replace: попарно заменяет подстроки в строке
    clear_data: устраняет пустые значения
    noise_restricts_check: обрабатывает зашумленные значения
    missing_values_check: обрабатывает пропущенные значения
    duplicates_check: обрабатывает дублирующиеся значения
    data_types_check: конвертирует поля набора данных
    ref_integrity: обеспечивает ссылочную целостность
    value_restrict_check: обрабатывает значения в полях набора данных в соответствии с ограничениями
    len_restricts_check: проверяет соответствие длины значений ограничениям
    check: последовательно выполняет все процедуры обработки и проверки данных
    """
    def __init__(self,
                 data: pd.DataFrame,
                 error_handling: Dict[str, Dict],
                 data_ref: Dict[str, pd.DataFrame] = None):
        """
        Конструирует необходимые аттрибуты.
        :param data: проверяемый набор данных.
        :param error_handling: параметры проверки качества данных.
        :param data_ref: набор данных из связанной таблицы.
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
    def data_type_regex(data_type: str) -> str:
        """
        Возвращает соответствующее регулярное выражение для указанного типа данных.
        :param data_type: наименование типа данных.
        :return: регулярное выражение.
        """
        if data_type == 'integer':
            return r"^\d+$"
        elif data_type == 'numeric':
            return r"[+-]?([0-9]*[.])?[0-9]+"
        elif data_type == 'text':
            return r"^(?!\s*$).+"
        elif data_type == 'timestamp':
            return r"[0-9]{1,2}.[0-9]{1,2}.[0-9]{1,4} [0-9]{1,2}:[0-9]{1,2}"
        elif data_type == 'date_excel':
            return r"^-?\d+(\.\d+)?$"

    @staticmethod
    def data_type_convert(data: pd.DataFrame, field: str, data_type: str) -> pd.Series:
        """
        Конвертирует данные указанного поля набора данных в определенный тип.
        :param data: набор исходных данных.
        :param field: наименование поля, конвертируемых данных.
        :param data_type: наименование типа данных.
        :return: поле набора данных с измененным типом.
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
    def multiple_replace(string: str, phrases: dict) -> str:
        """
        Проводит попарную замену символов в строке.
        :param string: обрабатываемая строка.
        :param phrases: словарь, где ключ - заменяемая подстрока,
        а его значение - заменяющая подстрока.
        :return: измененная строка.
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
        "Зашумленные" данные логирует.
        """
        noises_data = pd.DataFrame(columns=self.data.columns)
        for field in self.error_handling['noise']:
            # получение подстрок, заменяющих подстроки, удовлетворяющих регулярному выражению.
            regex = self.error_handling['noise'][field]['regex']
            match_replace = self.error_handling['noise'][field]['match_replace']
            # поиск подстрок, удовлетворяющих регулярному выражению.
            mask = self.data[field].str.contains(regex)
            mask.replace(np.nan, False, inplace=True)
            # получение выборки данных, содержащих найденные подстроки.
            noises_data = pd.concat([noises_data, self.data[mask]])

            if match_replace:
                # замена найденных подстрок.
                clean_field = self.data[mask][field]\
                    .apply((lambda x: DataQuality.multiple_replace(x, match_replace)))
                self.data.loc[mask, field] = clean_field

        # логирование некорректных данных.
        noises_data.drop_duplicates(inplace=True)
        noises_data["noises"] = True
        self.incorrect_data = pd.concat([self.incorrect_data, noises_data])

    def missing_values_check(self):
        """
        Устраняет из набора данных все строки если пусто
        хотя бы одно поле из критически важного набора полей таблицы.
        В случае, если пусто не критически важное поле - оно заполняется специальным значением.
        Некорректные данные логирует.
        """
        missing_handling = self.error_handling['missing']
        # поиск объектов набора данных, в которых хотя бы одно поле
        # из указанного набора ключевых полей (первичный ключ) содержит пустое значение.
        mask = self.data[missing_handling['drop']].isnull().any(axis=1)

        # логирование некорректных данных.
        missing_values = self.data[mask]
        missing_values["missing_values_check"] = True
        self.incorrect_data = pd.concat([self.incorrect_data, missing_values])

        # исключение данных с пропусками по ключевым полям.
        self.data = self.data[~mask]
        # наполнение пропусков по указанным полям значениями по умолчанию.
        if missing_handling['fill']:
            for field in missing_handling['fill']:
                self.data[field].replace('nan', missing_handling['fill'][field], inplace=True)
                self.data.loc[:, field] = self.data[field].fillna(missing_handling['fill'][field])

    def duplicates_check(self):
        """
        Устраняет дублирующиеся по указанному набору полей значения.
        Некорректные данные логирует.
        """
        # логирование дублирующихся данных по каждой указанной выборке полей.
        for fields in self.error_handling['duplicate']['log']:
            mask = self.data[fields].duplicated(keep=False)
            duplicates = self.data[mask]
            duplicates["duplicates_check"] = True
            self.incorrect_data = pd.concat([self.incorrect_data, duplicates])
            self.incorrect_data.drop_duplicates(inplace=True)

        # исключение дублирующихся данных по указанной выборке полей.
        self.data.drop_duplicates(subset=self.error_handling['duplicate']['drop'], inplace=True)

    def data_types_check(self):
        """
        На основе регулярного выражения определяет тип данных в полях,
        и затем значения конвертируются. Некорректные данные логирует.
        """
        mask = pd.DataFrame(columns=self.data.columns)
        # Поиск данных по каждому полю, удовлетворяющих регулярному выражению того типа данных,
        # которому они должны соответствовать.
        for field in self.error_handling['data_types']:
            regex = DataQuality.data_type_regex(self.error_handling['data_types'][field])
            mask[field] = self.data[field].str.match(regex)
        mask.replace(np.nan, False, inplace=True)

        # логирование данных, не прошедших проверку на соответствие типов.
        data_types_invalid = self.data[~mask]
        data_types_invalid.dropna(inplace=True)
        data_types_invalid["data_types_check"] = True
        self.incorrect_data = pd.concat([self.incorrect_data, data_types_invalid])

        # конвертирование типов данных по каждому полю.
        for field in self.error_handling['data_types']:
            self.data.loc[:, field] = DataQuality.\
                data_type_convert(self.data,
                                  field,
                                  self.error_handling['data_types'][field])

    def ref_integrity(self):
        """
        Устраняет из проверяемой таблицы те строки по указанному полю,
        которые отсутствуют в связанном наборе данных.
        """
        ref_integrity = self.error_handling['ref_integrity']
        # проверка наличия значений по ключевому полю по каждой родительской таблице.
        for table_ref in ref_integrity:
            data_union = self.data_ref[table_ref]\
                .merge(self.data,
                       on=ref_integrity[table_ref]['fields'],
                       how='outer',
                       indicator=True)

            # выборка только тех строк, значение которых
            # по полю внешнего ключа содержится в родительской таблице.
            self.data = data_union[data_union['_merge'] == 'both'].drop('_merge', axis=1)

            # логирование строк, содержавших значение в поле
            # внешнего ключа, которых нет в родительской таблице.
            ref_integrity_invalid = data_union[data_union['_merge'] == 'right_only']\
                .drop('_merge', axis=1)
            ref_integrity_invalid["ref_integrity_check"] = True
            self.incorrect_data = pd.concat([self.incorrect_data, ref_integrity_invalid])

    def value_restrict_check(self):
        """
        Проверяет значения на соответствие указанному критерию.
        Устраняет и логирует некорректные данные.
        """
        # проверка каждого указанного поля на содержание значений,
        # удовлетворяющих указанным ограничениям.
        for field in self.error_handling['val_restrict']:
            query = self.error_handling['val_restrict'][field]
            filed_data_type = self.error_handling['data_types'][field]
            field_data = DataQuality.data_type_convert(self.data,
                                                       field,
                                                       filed_data_type)
            mask = field_data.to_frame().eval(f'{field} != {field} or {query}')

            # поиск и логирование значений, не удовлетворяющих условиям.
            value_invalid = self.data[~mask]
            value_invalid["value_restrict_check"] = True
            self.incorrect_data = pd.concat([self.incorrect_data, value_invalid])

            # выборка строк со значениями, прошедших проверку.
            self.data = self.data[mask]

    def len_restricts_check(self):
        """
        Проверяет соответствие длины строки крайним значениям.
        Логирует подозрительные данные.
        """
        # проверка значений по каждому указанному полю на соответствие лимитам длины.
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
            # поиск значений с длинной, несоответствующей указанным ограничениям.
            mask = field_data.to_frame().eval(query)

            # логирование найденных некорректных данных.
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


def data_quality(table: str,
                 conn_info: Union[List[Dict], Dict[str, Dict]],
                 hook_from: PostgresHook,
                 hook_to: PostgresHook,
                 logs: dict,
                 error_handling: Dict[str, Dict]) -> None:
    """
    Запускает проверку качества данных и сохраняет обработанный набор данных.
    Перед запуском проверки получает данные связанных таблиц,
    на которые ссылается обрабатываемый набор данных по внешним ключам.
    :param conn_info: конфигурационные данные соединений.
    :param hook_from: хук соединения исходной базы данных.
    :param hook_to: хук соединения целевой базы данных.
    :param table: наименование проверяемой таблицы.
    :param logs: параметры журнала событий.
    :param error_handling: параметры проверки качества данных.
    """
    # получение исходных данных
    request_data = f"select * from {conn_info['tables'][table]['from']['schema']}.{table};"
    data_raw = hook_from.get_pandas_df(sql=request_data)

    # получение данных каждой родительской таблицы для последующей поверки ссылочной целостности
    refs = {}
    if error_handling['ref_integrity']:
        for table_ref in error_handling['ref_integrity']:
            conn_id = conn_info['tables'][table_ref]['to']['conn_id']
            schema = conn_info['tables'][table_ref]['to']['schema']
            fields_ref = error_handling['ref_integrity'][table_ref]['fields_ref']
            try:
                hook = PostgresHook(postgres_conn_id=conn_id)
                print(f"Postgres connects success to the {table_ref} table")
            except Exception as error:
                raise AirflowException(f"ERROR: Connect error with table {table_ref}: {error}")\
                    from error
            request_ref_data = f"select {', '.join(fields_ref)} from {schema}.{table_ref};"
            data_ref = hook.get_pandas_df(sql=request_ref_data)
            refs[table_ref] = data_ref

    data_checked = DataQuality(data=data_raw, data_ref=refs, error_handling=error_handling)
    data_checked.check()

    # загрузка обработанных данных в целевые таблицы и некорректных данных в таблицы логирования
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

    # логирование информации о загрузке таблицы
    upload_info = pd.DataFrame(data={'table_name': [table],
                                     'update_date': [datetime.now()],
                                     'load_id': [round(datetime.now().timestamp())]})
    upload_info.to_sql(name='upload_tables_tech',
                       schema=logs['schema'],
                       con=hook_to.get_sqlalchemy_engine(),
                       if_exists='append',
                       index=False)
    hook_to.get_conn().commit()


dq_params_path = expanduser("/opt/airflow/scripts/data_quality_params.json")

# Параметры подключения и проверки качества данных
with open(dq_params_path, 'r', encoding="utf-8") as json_file:
    dq_params = load(json_file)

# Формирование запроса на удаление всех данных из всех таблиц
remove_all_data = ''
for table_name in dq_params['conn_info']['tables']:
    table_schema = dq_params['conn_info']['tables'][table_name]['to']['schema']
    table_log = dq_params['log_info']['tables'][table_name]['table_log']
    table_log_schema = dq_params['log_info']['tables'][table_name]['schema']
    remove_all_data += f"TRUNCATE {table_schema}.{table_name} CASCADE;\n"
    remove_all_data += f"TRUNCATE {table_log_schema}.{table_log} CASCADE;\n"
