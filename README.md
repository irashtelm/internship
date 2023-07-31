# ETL-процесс

## Содержание проекта
* Директории:
1. AIRFLOW - содержит информацию об используемых переменных, конфигурационные файлы запуска Apache Airflow с помощью Docker, директории с DAG'ами и скриптами обработки данных.
2. POSTGRES - содержит DDL-скрипты, предназначенный для формирования всех сущностей DDS и datamarts-слоев.
4. DOCUMENTATION - содержит документацию по проекту.
* Файлы:
1. AIRFLOW/.env - информация о переменных среды AIRFLOW_UID и PYTHONPATH.
2. AIRFLOW/docker-compose.yml - измененный скрипт запуска Apache Airflow с помощью Docker с примонтированной директорией, хранящей python-скрипты.
3. AIRFLOW/variables.txt - используемые переменные в Apache Airflow.
4. AIRFLOW/dags/sources_to_dds.py - python-скрипт с DAG-ом, запускающим процедуру проверки качества данных.
5. AIRFLOW/dags/dds_to_dm.py - python-скрипт с DAG-ом, запускающим процедуру создания витрин данных.
6. AIRFLOW/dags/select_data_dag.py - python-скрипт с DAG-ом, запускающим процедуру возвращения заданного количества строк из целевой таблицы.
8. AIRFLOW/scripts/data_quality.py - python-скрипт с классом для проверки качества данных и загрузки данных из одной базы данных в другую.
9. AIRFLOW/scripts/data_marts.py - python-скрипт с классом для создания таблиц и витрин данных.
10. AIRFLOW/scripts/data_quality_params.json - набор параметров, испольуемый при наполнении данными dds-слоя.
11. AIRFLOW/scripts/data_marts_params.json - набор параметров, испольуемый при наполнении данными datamarts-слоя.
12. POSTGRES/DDL.sql - SQL-скрипт, с процедурами создания сущностей на слое DDS
13. POSTGRES/DDL.sql - SQL-скрипт, с процедурами создания сущностей на слое datamarts
14. DOCUMENTATION/АРХИТЕКТУРА РЕШЕНИЯ.docx - информация об архитектуре решения.

## Хранилище данных.
Хранилище данных представлено четырьмя слоями: слой управления данных, слой временного хранения данных,
слой подготовленных данных, слой витрин данных.

В слое управления данным указаны технологии, осуществляющие обработку данных. Docker осуществляет контейнеризацию Airflow с Python. При этом, Airflow оркестрирует исполнение двух Python-скриптов, содержащих логику обработки данных, и двух python-скриптов, предназначенных для запуска DAG’ов.

Первый скрипт предназначен для трансформации и очистки исходных данных и их последующую загрузку в схему подготовленных данных операционной базы данных. Данные, непрошедшие проверку качества попадают в таблицы логирования, размещенные на схеме некорректных данных.

Второй python-скрипт на основе полученных данных формирует и загружает витрины данных. Формирование витрин происходит в два этапа: сперва создаются таблицы соединения, а затем, на их основе, создаются представления. Образованные витрины данных визуализируются с помощью инструмента создания чартов и дашбордов Superset, который, в свою очередь, контейнеризирован с помощью Docker.



![Схема архитектурного решения](https://github.com/asetimankulov/internship/assets/98170451/52a84b9c-4a53-4255-94b0-24d0a8ed59cf)



## Трансформация, очистка и загрузка данных.

Первый набор python-скриптов, предназначенный для трансформации, очистки и загрузки исходных "сырых" данных, содержит DAG, запускающий процедуры
проверки качества загружаемых данных и их преобразования, и файл с классом проверки качества данных и процедурами загрузки и обработки данных.

DAG запускает исполнение 10 тасок. При этом, параллельное исполнение всех тасок нереализуемо в виду наличия ограничений на ссылочную целостность
связанных таблиц. Поэтому реализована следующая последовательность исполнения тасок.

![image](https://github.com/asetimankulov/internship/assets/98170451/7d236256-0c15-451c-b75a-10fd6b6a9bc1)

где:
* start_step и end_step - операторы, ничего не исполняющие.
* layer_recreation - оператор Postgres, пересоздающий весь слой DDS (удаление схем со всеми объектами и последующее их создание).
* brand_upload, category_upload, stores_upload, product_upload, transaction_stores_upload, stock_upload, transaction_upload - операторы Python,
исполняющие процедуры проверки качества соответствующих таблиц.

Модуль data_quality содержит класс проверки данных таблицы по различным критериям качества данных.
Класс содержит следующие методы проверки качества данных:
* noise_restricts_check - логирование и устранение "шумов" в данных. С помощью регулярного выражения выявляется наличие/отсутствие "шумов"
и происходит их устранение.
* data_types_check - типы обрабатываемых данных проверяются на их соответствие полям таблицы загрузки. Некорректные данные заменяются
на пропуск и логгируются.
* missing_values_check - проверка наличия/отсутствия пропусков и их обработка. Отсутствие данных по полям первичного ключа приводит к устранению данных, в остальных случаях - данные остаются без изменений или заменяютя на указанное значение.
* duplicates_check - устранение дубликатов по всем полям или первичному ключу. Некорректные данные логгируются.
* value_restrict_check - проверяет соответствие значения указанного поля таблицы некоторым ограничениям. Результат несоответствия логгируется.
* len_restricts_check - проверяет длину указанного поля и логгирует те строки, которые нарушают ограничение.
* ref_integrity - проверка, предназначенная для сохранения ссылочной целостности. В родительские таблицы, перед их загрузкой, добавляются отсутствующие в них, но имеющиеся в их дочерних таблицах значения.

Для каждой таблицы сформированы свои требования к качеству данных. Хранение набора параметров для каждой таблицы реализовано следующим образом:

    'category': {
        'missing': {'drop': ['category_id'],
                    'fill': {'category_name': 'Категория не определена'}},
        'duplicate': ['category_id'],
        'noise': {'category_name': {"regex": "_", "match_replace": {"_": " "}}},
        'len_restrict': {'category_name': {'min': 2, 'max': None}},
        'data_types': {'category_id': 'text',
                       'category_name': 'text'},
        'ref_integrity': {'product': {'field': 'category_id',
                                      'field_ref': 'category_id',
                                      'conn_ref': conn_info['product']['from']}}

В случае с таблицей "category", проверка качества реализована так:
* Обработка пропусков ("missing"): пропуск по "category_id" приводит к удалению строки, пропуски по полю "category_name", заполняются значением "Категория не определена".
* Обработка дубликатов: дублем считается строка, совпадающая по полю "category_id".
* Обработка шумов ("noise"): значения поля "category_name" проверяются регулярным выражением "\_". Некорректный символ "\_" заменяется на " ".
* Обработка длины значений ("len_restrict"): значение по полю "category_name" будет залоггировано в случае, если его длина  < 2.
* Обработка типов данных ("data_types"): значения полей "category_id" и "category_name" проверяются на соответствие типу данных text.
* Обеспечение ссылочной целостности ("ref_integrity"): из таблицы "product" в таблицу "category" добавляются значения поля "category_id", отсутствующие в таблице "category" и присутствующие в таблице "product".










