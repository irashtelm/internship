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

