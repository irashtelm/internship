"""Подключение необходимых библиотек."""
from datetime import datetime

from airflow import AirflowException
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
import pandas as pd
import numpy as np


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
        "Зашумленные" данные логгируются.
        """
        noises_data = pd.DataFrame(columns=self.data.columns)
        for field in self.error_handling['noise']:
            regex = self.error_handling['noise'][field]['regex']
            match_replace = self.error_handling['noise'][field]['match_replace']
            mask = self.data[field].str.contains(regex)
            mask.replace(np.nan, False, inplace=True)

            if match_replace:
                clean_field = self.data[mask][field].apply((lambda x:
                                                            DataQuality.multiple_replace(x, match_replace)))
                self.data.loc[mask, field] = clean_field
            else:
                noises_data = pd.concat([noises_data, self.data[mask]])
        noises_data.drop_duplicates(inplace=True)
        noises_data["noises"] = True
        self.incorrect_data = pd.concat([self.incorrect_data, noises_data])

    def missing_values_check(self):
        """
        Устраняет из набора данных все строки если пусто
        хотя бы одно поле из критически важного набора полей таблицы.
        В случае, если пусто не критически важное поле - он заполняется специальным значением.
        Некорректные данные логгируются.
        """
        missing_handling = self.error_handling['missing']
        mask = self.data[missing_handling['drop']].isnull().any(axis=1)
        self.data = self.data[~mask]
        for field in missing_handling['fill']:
            self.data[field].replace('nan', missing_handling['fill'][field], inplace=True)
            self.data.loc[:, field] = self.data[field].fillna(missing_handling['fill'][field])

        missing_values = self.data[mask]
        missing_values["missing_values_check"] = True
        self.incorrect_data = pd.concat([self.incorrect_data, missing_values])

    def duplicates_check(self):
        """
        Устраняются дублирующиеся по указанному набору полей значения.
        Некорректные данные логгируются.
        """

        if 'duplicate_log' in self.error_handling:
            for fields in self.error_handling['duplicate_log']:
                mask = (self.data[fields].duplicated(keep=False))
                duplicates = self.data[mask]
                duplicates["duplicates_check"] = True

        fields = self.error_handling['duplicate']
        mask = (self.data.duplicated(subset=fields, keep=False))
        duplicates = self.data[mask]
        duplicates["duplicates_check"] = True

        self.incorrect_data = pd.concat([self.incorrect_data, duplicates])
        self.incorrect_data.drop_duplicates(inplace=True)
        self.data.drop_duplicates(subset=fields, inplace=True)

    def data_types_check(self):
        """
        На основе регулярного выражения определяется тип данных в полях,
        и затем значения конвертируются.
        Некорректные данные логгируются.
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
        В проверяемую таблицу добавляются те строки по указанному полю,
        которые отсутствуют в проверяемом наборе данных, но присутствуют в связанном.
        """

        ref_integrity = self.error_handling['ref_integrity']
        complements = pd.DataFrame(columns=self.data.columns)
        for table_ref in ref_integrity:
            data_union = self.data_ref[table_ref].merge(self.data,
                                                        left_on=ref_integrity[table_ref]['field_ref'],
                                                        right_on=ref_integrity[table_ref]['field'],
                                                        how='left',
                                                        indicator=True)

            complement = data_union[data_union['_merge'] == 'left_only'].drop('_merge', axis=1)
            replace_value = ''
            complement.replace('nan', replace_value, inplace=True)
            complement.replace(np.nan, replace_value, inplace=True)
            complements = pd.concat([complements, complement])

        if not complements.empty:
            complements.drop_duplicates(inplace=True)
            self.data = pd.concat([self.data, complements], ignore_index=True)

    def value_restrict_check(self):
        """
        Проверка значений на соответствие указанному критерию.
        Некорректные данные логгируются.
        """
        for field in self.error_handling['value_restrict']:
            query = self.error_handling['value_restrict'][field]
            field_data = DataQuality.data_type_convert(self.data, field, self.error_handling['data_types'][field])
            mask = field_data.to_frame().eval(f'{field} != {field} or {query}')
            # self.data=self.data.query(mask)
            value_invalid = self.data[~mask]
            value_invalid["value_restrict_check"] = True
            self.incorrect_data = pd.concat([self.incorrect_data, value_invalid])

    def len_restricts_check(self):
        """
        Проверяется соответствие длины строки крайним значениям.
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
            # self.data = self.data[~mask]
            # сохраняем некорректные данные
            len_invalid = self.data[mask]
            len_invalid["len_restrict_check"] = True
            self.incorrect_data = pd.concat([self.incorrect_data, len_invalid])

    def check(self):
        """
        Выполняет все процедуры класса.
        """
        if self.data_ref:
            self.ref_integrity()
        self.clear_data()
        if 'noise' in self.error_handling:
            self.noise_restricts_check()
        self.data_types_check()
        self.missing_values_check()
        self.duplicates_check()
        if 'value_restrict' in self.error_handling:
            self.value_restrict_check()
        if 'len_restrict' in self.error_handling:
            self.len_restricts_check()


def data_quality(conn_from,
                 conn_to,
                 table,
                 logs,
                 error_handling):
    """
    Запускает проверку качества данных и сохраняет обработанный набор данных.
    Перед запуском проверки получает данные связанных таблиц.
    :param conn_from: конфигурационные данные соединения с отправителем данных.
    :param conn_to: конфигурационные данные соединения с получателем данных.
    :param table: наименование проверяемой таблицы.
    :param logs: параметры логгирования.
    :param error_handling: параметры проверки качества данных.
    """
    data_raw = conn_from['hook'].get_pandas_df(sql=f"select * from {conn_from['schema']}.{table};")
    refs = None
    if 'ref_integrity' in error_handling:
        for table_ref in error_handling['ref_integrity']:
            conn_id = error_handling['ref_integrity'][table_ref]['conn_ref']['conn_id']
            schema = error_handling['ref_integrity'][table_ref]['conn_ref']['schema']
            field_ref = error_handling['ref_integrity'][table_ref]['field_ref']
            hook = PostgresHook(postgres_conn_id=conn_id)
            query = f"select {field_ref} from {schema}.{table_ref};"
            data_ref = hook.get_pandas_df(sql=query)
            if not refs:
                refs = {table_ref: data_ref}
            else:
                refs[table_ref] = data_ref

    data = DataQuality(data=data_raw, data_ref=refs, error_handling=error_handling)
    data.check()

    data.data.to_sql(name=table,
                     schema=conn_to['schema'],
                     con=conn_to['hook'].get_sqlalchemy_engine(),
                     if_exists='append',
                     index=False)

    data.incorrect_data.to_sql(name=logs['table_log'],
                               schema=logs['schema'],
                               con=conn_to['hook'].get_sqlalchemy_engine(),
                               if_exists='append',
                               index=False)

    conn_to['hook'].get_conn().commit()


# Параметры подключения и хранения данных
conn_info_to = {'conn_id': 'dds_id', 'schema': 'dds'}  # {'conn_id': 'dds_id', 'schema': 'dds_dev'}
conn_info = {
    'brand': {'from': {'conn_id': 'sources_id', 'schema': 'sources'}, 'to': conn_info_to},
    'category': {'from': {'conn_id': 'sources_id', 'schema': 'sources'}, 'to': conn_info_to},
    'stores': {'from': {'conn_id': 'dds_id', 'schema': 'sources'}, 'to': conn_info_to},
    'product': {'from': {'conn_id': 'sources_id', 'schema': 'sources'}, 'to': conn_info_to},
    'transaction_stores': {'from': {'conn_id': 'dds_id', 'schema': 'sources'}, 'to': conn_info_to},
    'stock': {'from': {'conn_id': 'sources_id', 'schema': 'sources'}, 'to': conn_info_to},
    'transaction': {'from': {'conn_id': 'sources_id', 'schema': 'sources'}, 'to': conn_info_to}
}

# Параметры логгирования
tables_log_schema = 'data_quality'
tables_log = {
    'brand': {'table_log': 'brand_log', 'schema': tables_log_schema},
    'category': {'table_log': 'category_log', 'schema': tables_log_schema},
    'stores': {'table_log': 'stores_log', 'schema': tables_log_schema},
    'product': {'table_log': 'product_log', 'schema': tables_log_schema},
    'transaction_stores': {'table_log': 'transaction_stores_log', 'schema': tables_log_schema},
    'stock': {'table_log': 'stock_log', 'schema': tables_log_schema},
    'transaction': {'table_log': 'transaction_log', 'schema': tables_log_schema}
}

# Запрос на пересоздание слоя хранения обработанных данных
layer_recreation_query = f'''
DROP SCHEMA if exists {conn_info_to['schema']} CASCADE;
DROP SCHEMA if exists {tables_log_schema} CASCADE;

CREATE SCHEMA {conn_info_to['schema']};
CREATE SCHEMA {tables_log_schema};

CREATE TABLE {conn_info['brand']['to']['schema']}.brand ( 
brand_id integer PRIMARY KEY,
brand text
);

INSERT INTO {conn_info['brand']['to']['schema']}.brand
VALUES (-1, 'Бренд не определён');

CREATE TABLE {conn_info['category']['to']['schema']}.category (
category_id text PRIMARY KEY,
category_name text
);

INSERT INTO {conn_info['category']['to']['schema']}.category
VALUES ('-1', 'Категория не определена');

CREATE TABLE {conn_info['stores']['to']['schema']}.stores (
pos text PRIMARY KEY,
pos_name text
);

INSERT INTO {conn_info['stores']['to']['schema']}.stores
VALUES ('-1', 'Магазин не определён');

CREATE TABLE {conn_info['product']['to']['schema']}.product (
product_id integer PRIMARY KEY,
name_short text,
category_id text,
pricing_line_id integer,
brand_id integer,
CONSTRAINT fk_product_category FOREIGN KEY (category_id)
REFERENCES {conn_info['category']['to']['schema']}.category (category_id),
CONSTRAINT fk_product_brand FOREIGN KEY (brand_id)
REFERENCES {conn_info['brand']['to']['schema']}.brand (brand_id)
);

CREATE TABLE {conn_info['transaction_stores']['to']['schema']}.transaction_stores (
transaction_id text PRIMARY KEY,
pos text
);

CREATE TABLE {conn_info['stock']['to']['schema']}.stock (
available_on date,
product_id integer,
pos text,
available_quantity numeric NOT NULL,
cost_per_item numeric NOT NULL,
CONSTRAINT pk_stock PRIMARY KEY (available_on, product_id, pos),
CONSTRAINT fk_stock_product FOREIGN KEY (product_id)
REFERENCES {conn_info['product']['to']['schema']}.product (product_id)
);

CREATE TABLE {conn_info['transaction']['to']['schema']}.transaction (
transaction_id text,
product_id integer,
recorded_on timestamp NOT NULL,
quantity numeric NOT NULL,
price numeric NOT NULL,
price_full numeric NOT NULL,
order_type_id text,
CONSTRAINT pk_transaction PRIMARY KEY (transaction_id, product_id),
CONSTRAINT fk_transaction_product FOREIGN KEY (product_id)
REFERENCES {conn_info['product']['to']['schema']}.product (product_id)
);

-- Создание журналов событий
CREATE TABLE {tables_log['brand']['schema']}.{tables_log['brand']['table_log']} (
brand_id text,
brand text,
noises text,
missing_values_check text,
duplicates_check text,
data_types_check text,
value_restrict_check text,
len_restrict_check text
);

CREATE TABLE {tables_log['category']['schema']}.{tables_log['category']['table_log']}  (
category_id text,
category_name text,
noises text,
missing_values_check text,
duplicates_check text,
data_types_check text,
value_restrict_check text,
len_restrict_check text
);

CREATE TABLE {tables_log['stores']['schema']}.{tables_log['stores']['table_log']}  (
pos text,
pos_name text,
noises text,
missing_values_check text,
duplicates_check text,
data_types_check text,
value_restrict_check text,
len_restrict_check text
);

CREATE TABLE {tables_log['product']['schema']}.{tables_log['product']['table_log']}  (
product_id text,
name_short text,
category_id text,
pricing_line_id text,
brand_id text,
noises text,
missing_values_check text,
duplicates_check text,
data_types_check text,
value_restrict_check text,
len_restrict_check text
);

CREATE TABLE {tables_log['transaction_stores']['schema']}.{tables_log['transaction_stores']['table_log']} (
transaction_id text,
pos text,
noises text,
missing_values_check text,
duplicates_check text,
data_types_check text,
value_restrict_check text,
len_restrict_check text
);

CREATE TABLE {tables_log['stock']['schema']}.{tables_log['stock']['table_log']} (
available_on text,
product_id text,
pos text,
available_quantity text,
cost_per_item text,
noises text,
missing_values_check text,
duplicates_check text,
data_types_check text,
value_restrict_check text,
len_restrict_check text
);

CREATE TABLE {tables_log['transaction']['schema']}.{tables_log['transaction']['table_log']} (
transaction_id text,
product_id text,
recorded_on text,
quantity text,
price text ,
price_full text,
order_type_id text,
noises text,
missing_values_check text,
duplicates_check text,
data_types_check text,
value_restrict_check text,
len_restrict_check text
);
'''

# Параметры проверки качества данных
error_handling = {
    'brand': {
        'missing': {'drop': ['brand_id'],
                    'fill': {'brand': 'Бренд не определён'},
                    'fill_ref': {'brand_id': '-1'}},
        'duplicate': ['brand_id'],
        'noise': {'brand': {"regex": "═", "match_replace": {"═": " "}}},
        'len_restrict': {'brand': {'min': 2, 'max': None}},
        'data_types': {'brand_id': 'integer',
                       'brand': 'text'},
        'ref_integrity': {'product': {'field': 'brand_id',
                                      'field_ref': 'brand_id',
                                      'conn_ref': conn_info['product']['from']}}
    },
    'category': {
        'missing': {'drop': ['category_id'],
                    'fill': {'category_name': 'Категория не определена'},
                    'fill_ref': {'category_id': '-1'}},
        'duplicate': ['category_id'],
        'noise': {'category_name': {"regex": "_", "match_replace": {"_": " "}}},
        'len_restrict': {'category_name': {'min': 2, 'max': None}},
        'data_types': {'category_id': 'text',
                       'category_name': 'text'},
        'ref_integrity': {'product': {'field': 'category_id',
                                      'field_ref': 'category_id',
                                      'conn_ref': conn_info['product']['from']}}
    },
    'product': {
        'missing': {'drop': ['product_id'],
                    'fill': {'name_short': 'Товар не определён',
                             'category_id': '-1',
                             'brand_id': '-1'}},
        'duplicate': ['product_id'],
        'duplicate_log': [['name_short', 'brand_id']],
        'noise': {'name_short': {"regex": "^[0-9]*$", "match_replace": None},
                  'product_id': {"regex": "^[a-zA-Z]*$", "match_replace": None}},
        'len_restrict': {'name_short': {'min': 2, 'max': None}},
        'data_types': {'product_id': 'integer', 'name_short': 'text',
                       'category_id': 'text', 'pricing_line_id': 'integer',
                       'brand_id': 'integer'},
        'ref_integrity': {'transaction': {'field': 'product_id',
                                          'field_ref': 'product_id',
                                          'conn_ref': conn_info['transaction']['from']},
                          'stock': {'field': 'product_id',
                                    'field_ref': 'product_id',
                                    'conn_ref': conn_info['stock']['from']}}
    },
    'stock': {
        'missing': {'drop': ['available_on', 'product_id', 'pos', 'available_quantity', 'cost_per_item'],
                    'fill': {}},
        'duplicate': ['available_on', 'product_id', 'pos'],
        'len_restrict': {'pos': {'min': 2, 'max': None}},
        'value_restrict': {'available_quantity': '(available_quantity >= 0)',
                           'cost_per_item': '(cost_per_item >= 0)'},
        'data_types': {'available_on': 'date_excel',
                       'product_id': 'integer',
                       'pos': 'text',
                       'available_quantity': 'numeric',
                       'cost_per_item': 'numeric'}
    },
    'transaction': {
        'missing': {'drop': ['transaction_id', 'product_id', 'recorded_on', 'quantity', 'price', 'price_full', 'price'],
                    'fill': {}},
        'duplicate': ['transaction_id', 'product_id'],
        'value_restrict': {'quantity': '(quantity > 0)',
                           'price': '(price >= 0)',
                           'price_full': '(price_full > 0)'},
        'data_types': {'transaction_id': 'text',
                       'product_id': 'integer',
                       'recorded_on': 'timestamp',
                       'quantity': 'numeric',
                       'price': 'numeric',
                       'price_full': 'numeric',
                       'order_type_id': 'text'}
    },
    'transaction_stores': {
        'missing': {'drop': ['transaction_id'],
                    'fill': {'pos': '-1'}},
        'duplicate': ['transaction_id'],
        'len_restrict': {'pos': {'min': 2, 'max': None}},
        'data_types': {'transaction_id': 'text',
                       'pos': 'text'},
        'ref_integrity': {'transaction': {'field': 'transaction_id',
                                          'field_ref': 'transaction_id',
                                          'conn_ref': conn_info['transaction']['from']}}
    },
    'stores': {
        'missing': {'drop': ['pos'],
                    'fill': {'pos_name': 'Магазин не определён'},
                    'fill_ref': {'pos': '-1'}},
        'duplicate': ['pos'],
        'len_restrict': {'pos_name': {'min': 2, 'max': None}},
        'data_types': {'pos': 'text',
                       'pos_name': 'text'},
        'ref_integrity': {'stock': {'field': 'pos',
                                    'field_ref': 'pos',
                                    'conn_ref': conn_info['stock']['from']},
                          'transaction_stores': {'field': 'pos',
                                                 'field_ref': 'pos',
                                                 'conn_ref': conn_info['transaction_stores']['from']}}
    }
}

if __name__ == "__main__":
    main()