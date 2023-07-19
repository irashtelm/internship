# ETL-процесс

## Содержание проекта
* Директории:
1. AIRFLOW - содержит скрипт анализа качества данных, DAG на запуск поверки качества и DAG на осуществление процедуры select-а.
2. POSTGRES - содержит DDL-скрипт, предназначенный для формирования всех сущностей DDS-слоя.
* Файлы:
1. AIRFLOW/data_quality.py - python-скрипт с классом для проверки качества данных и загрузки данных из одной базы данных в другую.
2. AIRFLOW/upload_data_to_dds.py - python-скрипт с DAG-ом, запускающим процедуру проверки качества данных.
3. AIRFLOW/select_data_dag.py - python-скрипт с DAG-ом, запускающим процедуру возвращения заданного количества строк из целевой таблицы.
4. AIRFLOW/variables.txt - файл, с данными об используюшихся в AIRFLOW переменных.
5. POSTGRES/DDL.sql - SQL-скрипт, с процедурами создания сущностей на слое DDS

## Хранилище данных.
Хранилище данных представлено четырьмя слоями: слой управления данных, слой временного хранения данных,
слой подготовленных данных, слой витрин данных.
В слое управления данным указаны технологии, осуществляющие обработку данных. Docker осуществляет контейнеризацию Airflow с Python.
При этом, Airflow оркестрирует исполнение двух наборов Python-скриптов, один из которых предназначен для трансформации и очистки исходных
данных и их последующую загрузку в схему подготовленных данных операционной базы данных.
Далее, второй набор python-скриптов на основе полученных данных формирует и загружает витрины данных. Образованные витрины данных
визуализируются с помощью инструмента создания чартов и дашбордов Superset, который, в свою очередь, контейнеризирован с помощью Docker.


![Схема архитектурного решения](https://github.com/asetimankulov/internship/assets/98170451/113a49fe-1646-446c-980b-fa714f21d505)


## Трансформация, очистка и загрузка данных.

Первый набор python-скриптов, предназначенный для трансформации, очистки и загрузки исходных "сырых" данных, содержит DAG, запускающий процедуры
проверки качества загружаемых данных и их преобразования, и файл с классом проверки качества данных и процедурами загрузки и обработки данных.

DAG запускает исполнение 10 тасок. При этом, параллельное исполнение всех тасок нереализуемо в виду наличия ограничений на ссылочную целостность
связанных таблиц. Поэтому, реализована следующая последовательность исполнения тасок.

![image](https://github.com/asetimankulov/internship/assets/98170451/7d236256-0c15-451c-b75a-10fd6b6a9bc1)

где:
* start_step и end_step - операторы, ничего не исполняющие.
* layer_recreation - оператор Postgres, пересоздающий весь слой DDS (удаление схем со всеми объектами и последующее их создание).
* brand_upload, category_upload, stores_upload, product_upload, transaction_stores_upload, stock_upload, transaction_upload - операторы Python,
исполняющие процедуры проверки качества соответствующих таблиц.

Модуль data_quality содержит класс по проверки данных таблицы по различным критериям качества данных.
Класс содержит следующие методы проверки качества данных:
* noise_restricts_check - логирование и устранение "шумов" в данных. С помощью регулярного выраженния выявляется наличие/отсутствие "шумов"
и происходит их устранение.
* data_types_check - типы обрабатываемых данных проверяются на их соответствие полям таблицы загрузки. Неккоректные данные заменяются
на пропус и логгируются.
* missing_values_check - проверка наличия/отсутствия пропусков и их обработка. Отсутствие данных по полям первичного ключа приводит к устранению данных, в остальных случаях - данные остаются без изменений или заменяютя на указанное значение.
* duplicates_check - устранение дубликатов по всем полям или первичному ключу. Некорректные данные логгируются.
* value_restrict_check - проверяет соответствие значения укзанного поля таблицы некоторым ограничениям. Результат несоответствия логгируется.
* len_restricts_check - проверяет длину указанного поля и логгирует те строки, которые нарушают ограничение.
* ref_integrity - проверка, предназначенная для сохранения ссылочной целостности. В родительские таблицы, перед их загрузкой, добавляются отсутсвующие в них, но имеющиеся в их дочерних таблицах значения.

Для каждой таблицы сформированы свои требования к качеству данных. Хранение набора параметров для каждой таблицы реализован следующим образом:

    'product': {
        'missing': {'drop': ['product_id'],
                    'fill': {'name_short': 'Товар не определён',
                             'category_id': '-1', 'brand_id': '-1'}},
        'duplicate': ['product_id'],
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
    }












