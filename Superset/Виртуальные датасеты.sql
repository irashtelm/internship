-- Виртуальный датасет для чарта "Доля остатков в штуках (по категориям), %"
{% set магазин = filter_values('Магазин') %}
{% set категория = filter_values('Категория') %}
{% set товар = filter_values('Товар') %}
{% set бренд = filter_values('Бренд') %}
{% set дата = filter_values('Дата') %}

{% set where_clause = "" %}

{% if магазин %}
    {% set where_clause = where_clause + " AND \"Магазин\" IN ('" + "','".join(магазин) + "')" %}
{% endif %}

{% if категория %}
    {% set where_clause = where_clause + " AND \"Категория\" IN ('" + "','".join(категория) + "')" %}
{% endif %}

{% if товар %}
    {% set where_clause = where_clause + " AND \"Товар\" IN ('" + "','".join(товар) + "')" %}
{% endif %}

{% if бренд %}
    {% set where_clause = where_clause + " AND \"Бренд\" IN ('" + "','".join(бренд) + "')" %}
{% endif %}

{% if дата %}
    {% set where_clause = where_clause + " AND \"Дата\" >= '" + дата[0].isoformat() + "' AND \"Дата\" <= '" + дата[1].isoformat() + "'" %}
{% endif %}

WITH table_rnum AS (
    SELECT "Дата наличия товара", "Категория",
        SUM("Доступное количество товара, шт.") AS sum_col,
        ROW_NUMBER() OVER (partition by "Дата наличия товара" ORDER BY SUM("Доступное количество товара, шт.") DESC) AS rnum
    FROM stock_data_mart
    WHERE 1=1 {{ where_clause }}
    GROUP BY "Дата наличия товара", "Категория"
),
table_raiting AS (
    SELECT "Дата наличия товара", "Категория",
        sum_col,
        CASE 
            WHEN rnum < 5 THEN "Категория"
            ELSE 'Остальные категории'
        END AS cat_group
    FROM table_rnum
)
select distinct "Дата наличия товара", "Категория", "Объем доступного количества товара по категориям, руб." from (
SELECT "Дата наличия товара", cat_group as "Категория",
    SUM(sum_col) OVER (PARTITION BY "Дата наличия товара", cat_group) AS "Объем доступного количества товара по категориям, руб."
FROM table_raiting) s;


-- Виртуальный датасет для чарта "Доля остатков в штуках (по брендам), %"
{% set магазин = filter_values('Магазин') %}
{% set категория = filter_values('Категория') %}
{% set товар = filter_values('Товар') %}
{% set бренд = filter_values('Бренд') %}
{% set дата = filter_values('Дата') %}

{% set where_clause = "" %}

{% if магазин %}
    {% set where_clause = where_clause + " AND \"Магазин\" IN ('" + "','".join(магазин) + "')" %}
{% endif %}

{% if категория %}
    {% set where_clause = where_clause + " AND \"Категория\" IN ('" + "','".join(категория) + "')" %}
{% endif %}

{% if товар %}
    {% set where_clause = where_clause + " AND \"Товар\" IN ('" + "','".join(товар) + "')" %}
{% endif %}

{% if бренд %}
    {% set where_clause = where_clause + " AND \"Бренд\" IN ('" + "','".join(бренд) + "')" %}
{% endif %}

{% if дата %}
    {% set where_clause = where_clause + " AND \"Дата\" >= '" + дата[0].isoformat() + "' AND \"Дата\" <= '" + дата[1].isoformat() + "'" %}
{% endif %}

WITH table_rnum AS (
    SELECT "Дата наличия товара", "Бренд",
        SUM("Доступное количество товара, шт.") AS sum_col,
        ROW_NUMBER() OVER (partition by "Дата наличия товара" ORDER BY SUM("Доступное количество товара, шт.") DESC) AS rnum
    FROM stock_data_mart
    WHERE 1=1 {{ where_clause }}
    GROUP BY "Дата наличия товара", "Бренд"
),
table_raiting AS (
    SELECT "Дата наличия товара", "Бренд",
        sum_col,
        CASE 
            WHEN rnum < 5 THEN "Бренд"
            ELSE 'Остальные бренды'
        END AS cat_group
    FROM table_rnum
)
select distinct "Дата наличия товара", "Бренд", "Объем доступного количества товара по брендам, руб." from (
SELECT "Дата наличия товара", cat_group as "Бренд",
    SUM(sum_col) OVER (PARTITION BY "Дата наличия товара", cat_group) AS "Объем доступного количества товара по брендам, руб."
FROM table_raiting) s;

-- Виртуальный датасет для чарта "Доля продаж в рублях (по категориям), %"

{% set магазин = filter_values('Магазин') %}
{% set категория = filter_values('Категория') %}
{% set товар = filter_values('Товар') %}
{% set бренд = filter_values('Бренд') %}
{% set дата = filter_values('Дата') %}

{% set where_clause = "" %}

{% if магазин %}
    {% set where_clause = where_clause + " AND \"Магазин\" IN ('" + "','".join(магазин) + "')" %}
{% endif %}

{% if категория %}
    {% set where_clause = where_clause + " AND \"Категория\" IN ('" + "','".join(категория) + "')" %}
{% endif %}

{% if товар %}
    {% set where_clause = where_clause + " AND \"Товар\" IN ('" + "','".join(товар) + "')" %}
{% endif %}

{% if бренд %}
    {% set where_clause = where_clause + " AND \"Бренд\" IN ('" + "','".join(бренд) + "')" %}
{% endif %}

{% if дата %}
    {% set where_clause = where_clause + " AND \"Дата\" >= '" + дата[0].isoformat() + "' AND \"Дата\" <= '" + дата[1].isoformat() + "'" %}
{% endif %}

WITH table_rnum AS (
    SELECT DATE("Дата и время транзакции") as "Дата транзакции", "Категория",
        SUM("Сумма заказа после скидки, руб.") AS sum_col,
        ROW_NUMBER() OVER (partition by DATE("Дата и время транзакции") ORDER BY SUM("Сумма заказа после скидки, руб.") DESC) AS rnum
    FROM orders_data_mart
    WHERE 1=1 {{ where_clause }}
    GROUP BY DATE("Дата и время транзакции"), "Категория"
),
table_raiting AS (
    SELECT "Дата транзакции", "Категория",
        sum_col,
        CASE 
            WHEN rnum < 5 THEN "Категория"
            ELSE 'Остальные категории'
        END AS cat_group
    FROM table_rnum
)
select distinct "Дата транзакции", "Категория", "Объем продаж по категории, руб." from (
SELECT "Дата транзакции", cat_group as "Категория",
    SUM(sum_col) OVER (PARTITION BY "Дата транзакции", cat_group) AS "Объем продаж по категории, руб."
FROM table_raiting) s;


-- Виртуальный датасет для чарта "Доля продаж в рублях (по брендам), %"

{% set магазин = filter_values('Магазин') %}
{% set категория = filter_values('Категория') %}
{% set товар = filter_values('Товар') %}
{% set бренд = filter_values('Бренд') %}
{% set дата = filter_values('Дата') %}

{% set where_clause = "" %}

{% if магазин %}
    {% set where_clause = where_clause + " AND \"Магазин\" IN ('" + "','".join(магазин) + "')" %}
{% endif %}

{% if категория %}
    {% set where_clause = where_clause + " AND \"Категория\" IN ('" + "','".join(категория) + "')" %}
{% endif %}

{% if товар %}
    {% set where_clause = where_clause + " AND \"Товар\" IN ('" + "','".join(товар) + "')" %}
{% endif %}

{% if бренд %}
    {% set where_clause = where_clause + " AND \"Бренд\" IN ('" + "','".join(бренд) + "')" %}
{% endif %}

{% if дата %}
    {% set where_clause = where_clause + " AND \"Дата\" >= '" + дата[0].isoformat() + "' AND \"Дата\" <= '" + дата[1].isoformat() + "'" %}
{% endif %}

WITH table_rnum AS (
    SELECT DATE("Дата и время транзакции") as "Дата транзакции", "Бренд",
        SUM("Сумма заказа после скидки, руб.") AS sum_col,
        ROW_NUMBER() OVER (partition by DATE("Дата и время транзакции") ORDER BY SUM("Сумма заказа после скидки, руб.") DESC) AS rnum
    FROM orders_data_mart
    WHERE 1=1 {{ where_clause }}
    GROUP BY DATE("Дата и время транзакции"), "Бренд"
),
table_raiting AS (
    SELECT "Дата транзакции", "Бренд",
        sum_col,
        CASE 
            WHEN rnum < 5 THEN "Бренд"
            ELSE 'Остальные бренды'
        END AS cat_group
    FROM table_rnum
)
select distinct "Дата транзакции", "Бренд", "Объем продаж по категории, руб." from (
SELECT "Дата транзакции", cat_group as "Бренд",
    SUM(sum_col) OVER (PARTITION BY "Дата транзакции", cat_group) AS "Объем продаж по категории, руб."
FROM table_raiting) s;







