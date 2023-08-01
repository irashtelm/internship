-- Чарт "Прибыль и продажи, руб."
SELECT DATE_TRUNC('day', "Дата и время транзакции") AS "Дата и время транзакции",
       sum(("Цена товара после скидки, руб." - "Закупочная цена товара, руб.") * "Количество товара, шт.") AS "Прибыль, руб.",
       sum("Сумма заказа после скидки, руб.") AS "Продажи, руб."
FROM datamarts.orders_data_mart
GROUP BY DATE_TRUNC('day', "Дата и время транзакции")
ORDER BY "Прибыль, руб." DESC;

-- Чарт "Продажи, шт."
SELECT DATE_TRUNC('day', "Дата и время транзакции") AS "Дата и время транзакции",
       sum("Количество товара, шт.") AS "Продажи, шт."
FROM datamarts.orders_data_mart
GROUP BY DATE_TRUNC('day', "Дата и время транзакции")
ORDER BY "Продажи, шт." DESC;

-- Чарт "Продажи, шт."
SELECT DATE_TRUNC('day', "Дата и время транзакции") AS "Дата и время транзакции",
       sum("Количество товара, шт.") AS "Продажи, шт."
FROM datamarts.orders_data_mart
GROUP BY DATE_TRUNC('day', "Дата и время транзакции")
ORDER BY "Продажи, шт." DESC;

-- Чарт "Общая прибыль, руб."
SELECT SUM(("Цена товара после скидки, руб." - "Закупочная цена товара, руб.") * "Количество товара, шт.") AS "Общая прибыль, руб."
FROM datamarts.orders_data_mart;

-- Чарт "Общие продажи, руб."
SELECT sum("Сумма заказа после скидки, руб.") AS "Общие продажи, руб."
FROM datamarts.orders_data_mart;

-- Чарт "Общие продажи, шт."
SELECT sum("Количество товара, шт.") AS "Общие продажи, шт."
FROM datamarts.orders_data_mart;

-- Чарт "Среднедневные продажи, руб."
SELECT SUM("Сумма заказа после скидки, руб.")/COUNT(DISTINCT DATE("Дата и время транзакции")) AS "Среднедневные продажи, руб."
FROM datamarts.orders_data_mart;

-- Чарт "Средний чек, руб."
SELECT round(AVG("Сумма заказа после скидки, руб."), 1) AS "Средний чек, руб."
FROM datamarts.orders_data_mart;

-- Чарт "Топ-10 продаж, руб."
SELECT "Товар" AS "Товар",
       sum("Сумма заказа после скидки, руб.") AS "Топ продаж, руб."
FROM datamarts.orders_data_mart
GROUP BY "Товар"
ORDER BY "Топ продаж, руб." DESC;

-- Чарт "Низ-10 продаж, руб."
SELECT "Товар" AS "Товар",
       sum("Сумма заказа после скидки, руб.") AS "Низ продаж, руб."
FROM datamarts.orders_data_mart
GROUP BY "Товар"
ORDER BY "Низ продаж, руб." ASC;

-- Чарт "Доля продаж в рублях, %"
SELECT "Магазин" AS "Магазин",
       sum("Сумма заказа после скидки, руб.") AS "Доля продаж в рублях, %"
FROM datamarts.orders_data_mart
GROUP BY "Магазин"
ORDER BY "Доля продаж в рублях, %" DESC;

-- Чарт "Доля продаж в штуках, %"
SELECT "Магазин" AS "Магазин",
       sum("Количество товара, шт.") AS "SUM(Количество товара, шт.)"
FROM datamarts.orders_data_mart
GROUP BY "Магазин"
ORDER BY "SUM(Количество товара, шт.)" DESC;

-- Чарт "Доля продаж в рублях (по категориям), %"
SELECT "Категория" AS "Категория",
       sum("Объем продаж по категории, руб.") AS "SUM(Объем продаж по категории, руб.)"
FROM
  (WITH table_rnum AS
     (SELECT DATE("Дата и время транзакции") as "Дата транзакции",
             "Категория",
             SUM("Сумма заказа после скидки, руб.") AS sum_col,
             ROW_NUMBER() OVER (partition by DATE("Дата и время транзакции")
                                ORDER BY SUM("Сумма заказа после скидки, руб.") DESC) AS rnum
      FROM orders_data_mart
      WHERE 1=1
      GROUP BY DATE("Дата и время транзакции"),
               "Категория"),
        table_raiting AS
     (SELECT "Дата транзакции",
             "Категория",
             sum_col,
             CASE
                 WHEN rnum < 5 THEN "Категория"
                 ELSE 'Остальные категории'
             END AS cat_group
      FROM table_rnum) select distinct "Дата транзакции",
                                       "Категория",
                                       "Объем продаж по категории, руб."
   from
     (SELECT "Дата транзакции",
             cat_group as "Категория",
             SUM(sum_col) OVER (PARTITION BY "Дата транзакции",
                                             cat_group) AS "Объем продаж по категории, руб."
      FROM table_raiting) s) AS virtual_table
WHERE "Дата транзакции" IN (TO_DATE('2021-06-01', 'YYYY-MM-DD'))
GROUP BY "Категория"
ORDER BY "SUM(Объем продаж по категории, руб.)" DESC;

-- Чарт "Доля продаж в рублях (по брендам), %"
SELECT "Бренд" AS "Бренд",
       sum("Объем продаж по категории, руб.") AS "SUM(Объем продаж по категории, руб.)"
FROM
  (WITH table_rnum AS
     (SELECT DATE("Дата и время транзакции") as "Дата транзакции",
             "Бренд",
             SUM("Сумма заказа после скидки, руб.") AS sum_col,
             ROW_NUMBER() OVER (partition by DATE("Дата и время транзакции")
                                ORDER BY SUM("Сумма заказа после скидки, руб.") DESC) AS rnum
      FROM orders_data_mart
      WHERE 1=1
      GROUP BY DATE("Дата и время транзакции"),
               "Бренд"),
        table_raiting AS
     (SELECT "Дата транзакции",
             "Бренд",
             sum_col,
             CASE
                 WHEN rnum < 5 THEN "Бренд"
                 ELSE 'Остальные бренды'
             END AS cat_group
      FROM table_rnum) select distinct "Дата транзакции",
                                       "Бренд",
                                       "Объем продаж по категории, руб."
   from
     (SELECT "Дата транзакции",
             cat_group as "Бренд",
             SUM(sum_col) OVER (PARTITION BY "Дата транзакции",
                                             cat_group) AS "Объем продаж по категории, руб."
      FROM table_raiting) s) AS virtual_table
WHERE "Дата транзакции" IN (TO_DATE('2021-06-17', 'YYYY-MM-DD'))
GROUP BY "Бренд"
ORDER BY "SUM(Объем продаж по категории, руб.)" DESC;



















