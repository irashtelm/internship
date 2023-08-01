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























