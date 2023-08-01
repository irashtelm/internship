-- Чарт "Остатки, шт."
SELECT DATE_TRUNC('day', "Дата наличия товара") AS "Дата наличия товара",
       sum("Доступное количество товара, шт.") AS "Остатки, шт."
FROM datamarts.stock_data_mart
WHERE "Дата наличия товара" >= TO_DATE('2021-06-17', 'YYYY-MM-DD')
  AND "Дата наличия товара" < TO_DATE('2021-06-24', 'YYYY-MM-DD')
GROUP BY DATE_TRUNC('day', "Дата наличия товара")
ORDER BY "Остатки, шт." DESC;

-- Чарт "Остатки, руб."
SELECT DATE_TRUNC('day', "Дата наличия товара") AS "Дата наличия товара",
       sum("Сумма доступного остатка, руб.") AS "Остатки, руб."
FROM datamarts.stock_data_mart
WHERE "Дата наличия товара" >= TO_DATE('2021-06-17', 'YYYY-MM-DD')
  AND "Дата наличия товара" < TO_DATE('2021-06-24', 'YYYY-MM-DD')
GROUP BY DATE_TRUNC('day', "Дата наличия товара")
ORDER BY "Остатки, руб." DESC;

-- Чарт "Средняя закупочная цена, руб."
SELECT DATE_TRUNC('day', "Дата наличия товара") AS "Дата наличия товара",
       AVG("Закупочная цена товара, руб.") AS "Средняя закупочная цена, руб."
FROM datamarts.stock_data_mart
WHERE "Дата наличия товара" >= TO_DATE('2021-06-17', 'YYYY-MM-DD')
  AND "Дата наличия товара" < TO_DATE('2021-06-24', 'YYYY-MM-DD')
GROUP BY DATE_TRUNC('day', "Дата наличия товара")
ORDER BY "Средняя закупочная цена, руб." DESC;

-- Чарт "Общие остатки, шт."
SELECT sum("Доступное количество товара, шт.") AS "Общие остатки, шт."
FROM datamarts.stock_data_mart
WHERE "Дата наличия товара" >= TO_DATE('2021-06-17', 'YYYY-MM-DD')
  AND "Дата наличия товара" < TO_DATE('2021-06-24', 'YYYY-MM-DD');

-- Чарт "Общие остатки, руб."
SELECT sum("Сумма доступного остатка, руб.") AS "Общие остатки, руб."
FROM datamarts.stock_data_mart
WHERE "Дата наличия товара" >= TO_DATE('2021-06-17', 'YYYY-MM-DD')
  AND "Дата наличия товара" < TO_DATE('2021-06-24', 'YYYY-MM-DD');

-- Чарт "Средняя закупка, руб."
SELECT AVG("Закупочная цена товара, руб.") AS "AVG(Закупочная цена товара, руб.)"
FROM datamarts.stock_data_mart
WHERE "Дата наличия товара" >= TO_DATE('2021-06-17', 'YYYY-MM-DD')
  AND "Дата наличия товара" < TO_DATE('2021-06-24', 'YYYY-MM-DD');

-- Чарт "Топ-10 остатков, шт."
SELECT "Товар" AS "Товар",
       sum("Доступное количество товара, шт.") AS "Топ остатков, шт."
FROM datamarts.stock_data_mart
WHERE "Дата наличия товара" >= TO_DATE('2021-06-17', 'YYYY-MM-DD')
  AND "Дата наличия товара" < TO_DATE('2021-06-24', 'YYYY-MM-DD')
GROUP BY "Товар"
ORDER BY "Топ остатков, шт." DESC;

-- Чарт "Низ-10 остатков, шт."
SELECT "Товар" AS "Товар",
       sum("Доступное количество товара, шт.") AS "Низ остатков, шт."
FROM datamarts.stock_data_mart
WHERE "Дата наличия товара" >= TO_DATE('2021-06-17', 'YYYY-MM-DD')
  AND "Дата наличия товара" < TO_DATE('2021-06-24', 'YYYY-MM-DD')
GROUP BY "Товар"
ORDER BY "Низ остатков, шт." ASC;

-- Чарт "Доля остатков в штуках, %"
SELECT "Магазин" AS "Магазин",
       sum("Доступное количество товара, шт.") AS "Доля остатков в штуках, %"
FROM datamarts.stock_data_mart
WHERE "Дата наличия товара" >= TO_DATE('2021-06-17', 'YYYY-MM-DD')
  AND "Дата наличия товара" < TO_DATE('2021-06-24', 'YYYY-MM-DD')
GROUP BY "Магазин"
ORDER BY "Доля остатков в штуках, %" DESC;

-- Чарт "Доля остатков в рублях, %"
SELECT "Магазин" AS "Магазин",
       sum("Сумма доступного остатка, руб.") AS "SUM(Сумма доступного остатка, руб.)"
FROM datamarts.stock_data_mart
WHERE "Дата наличия товара" >= TO_DATE('2021-06-17', 'YYYY-MM-DD')
  AND "Дата наличия товара" < TO_DATE('2021-06-24', 'YYYY-MM-DD')
GROUP BY "Магазин"
ORDER BY "SUM(Сумма доступного остатка, руб.)" DESC;

-- Чарт "Доля остатков в штуках (по категориям), %"
SELECT "Категория" AS "Категория",
       sum("Объем доступного количества товар") AS "SUM(Объем доступного количества товар)"
FROM
  (WITH table_rnum AS
     (SELECT "Дата наличия товара",
             "Категория",
             SUM("Доступное количество товара, шт.") AS sum_col,
             ROW_NUMBER() OVER (partition by "Дата наличия товара"
                                ORDER BY SUM("Доступное количество товара, шт.") DESC) AS rnum
      FROM stock_data_mart
      WHERE 1=1
      GROUP BY "Дата наличия товара",
               "Категория"),
        table_raiting AS
     (SELECT "Дата наличия товара",
             "Категория",
             sum_col,
             CASE
                 WHEN rnum < 5 THEN "Категория"
                 ELSE 'Остальные категории'
             END AS cat_group
      FROM table_rnum) select distinct "Дата наличия товара",
                                       "Категория",
                                       "Объем доступного количества товара по категориям, руб."
   from
     (SELECT "Дата наличия товара",
             cat_group as "Категория",
             SUM(sum_col) OVER (PARTITION BY "Дата наличия товара",
                                             cat_group) AS "Объем доступного количества товара по категориям, руб."
      FROM table_raiting) s) AS virtual_table
WHERE "Дата наличия товара" IN (TO_DATE('2021-06-17', 'YYYY-MM-DD'))
GROUP BY "Категория"
ORDER BY "SUM(Объем доступного количества товар)" DESC;

-- Чарт "Доля остатков в штуках (по брендам), %"
SELECT "Бренд" AS "Бренд",
       sum("Объем доступного количества товар") AS "SUM(Объем доступного количества товар)"
FROM
  (WITH table_rnum AS
     (SELECT "Дата наличия товара",
             "Бренд",
             SUM("Доступное количество товара, шт.") AS sum_col,
             ROW_NUMBER() OVER (partition by "Дата наличия товара"
                                ORDER BY SUM("Доступное количество товара, шт.") DESC) AS rnum
      FROM stock_data_mart
      WHERE 1=1
      GROUP BY "Дата наличия товара",
               "Бренд"),
        table_raiting AS
     (SELECT "Дата наличия товара",
             "Бренд",
             sum_col,
             CASE
                 WHEN rnum < 5 THEN "Бренд"
                 ELSE 'Остальные бренды'
             END AS cat_group
      FROM table_rnum) select distinct "Дата наличия товара",
                                       "Бренд",
                                       "Объем доступного количества товара по брендам, руб."
   from
     (SELECT "Дата наличия товара",
             cat_group as "Бренд",
             SUM(sum_col) OVER (PARTITION BY "Дата наличия товара",
                                             cat_group) AS "Объем доступного количества товара по брендам, руб."
      FROM table_raiting) s) AS virtual_table
WHERE "Дата наличия товара" IN (TO_DATE('2021-06-17', 'YYYY-MM-DD'))
GROUP BY "Бренд"
ORDER BY "SUM(Объем доступного количества товар)" DESC;

