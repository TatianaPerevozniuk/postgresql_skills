--  ЧАСТЬ 1  --
-- ОПТИМИЗАЦИЯ БАЗЫ ДАННЫХ --

-- Задание 1
/* 
Клиенты сервиса начали замечать, что после нажатия на кнопку "Оформить заказ"
система на какое-то время подвисает. 
Скрипт для вставки данных в таблицу orders, 
которая хранит общую информацию о заказах смотрите ниже.
Чтобы лучше понять, как ещё используется в запросах таблица orders, 
выполните запросы из файла orders_stat.sql.
Проанализируем возможные причины медленной вставки новой строки в таблицу orders.
*/
BEGIN;

EXPLAIN (ANALYZE, BUFFERS)
INSERT INTO orders
    (order_id, order_dt, user_id, device_type, city_id, total_cost, discount, 
    final_cost)
SELECT MAX(order_id) + 1, current_timestamp, 
    '329551a1-215d-43e6-baee-322f2467272d', 
    'Desktop', 1, 1000.00, null, 1000.00
FROM orders;

-- Проверка вставленных данных
SELECT * FROM orders
WHERE user_id = '329551a1-215d-43e6-baee-322f2467272d'
ORDER BY order_id DESC;

-- Откат изменений
ROLLBACK;


------ РЕШЕНИЕ ------
--Проблема - медленная вставки новой строки в таблицу orders
-- Причина - избыточность индексов в таблице orders
-- Также необходимо поменять тип поля order_id,
-- чтобы id вычислялось автоматически
/* После запуска скриптов из файла orders_stat.sql, 
	   проверим какие индексы используются, а какие нет: 	*/

SELECT 
    schemaname,
	relname,
    indexrelid::regclass AS index_name,
    idx_scan,
    idx_tup_read,
    idx_tup_fetch
FROM
    pg_stat_user_indexes
WHERE
    schemaname = 'public'
    AND relname = 'orders'
	AND idx_scan = 0; -- индекс ни разу не использовался

-- Решение - удалим индексы, которые не используются
 /* Пимечание 
 	1.Обновление статистики:
	Если сервер недавно перезапускался, статистика индексов могла сброситься.
    Нужно выполнить анализ нагрузки перед удалением:
		ANALYZE VERBOSE orders;  
	2.Сделать резервную копию структуры индексов на случай, если потребуется их восстановление.*/

DROP INDEX IF EXISTS orders_city_id_idx,  
					  orders_device_type_city_id_idx,
					  orders_device_type_idx,
					  orders_discount_idx,
					  orders_final_cost_idx,
					  orders_total_cost_idx;

--Изменение типа данных order_id для автоинкрементации
-- Создаем последовательность для автоинкремента
CREATE SEQUENCE orders_order_id_seq;

-- Привязываем последовательность к столбцу order_id
ALTER TABLE orders
ALTER COLUMN order_id SET DEFAULT nextval('orders_order_id_seq');

-- Устанавливаем стартовое значение для последовательности
SELECT setval('orders_order_id_seq', COALESCE(MAX(order_id), 0)) FROM orders;

-- Добавляем ограничение уникальности для order_id, если его нет
ALTER TABLE orders
ADD CONSTRAINT orders_order_id_pk PRIMARY KEY (order_id);

--Теперь при вставке нового значения столбец order_id будет автоматически заполняться.
--Обновленный запрос на вставку данных
BEGIN;

EXPLAIN (ANALYZE, BUFFERS)
INSERT INTO orders
    (order_dt, user_id, device_type, city_id, total_cost, discount, final_cost)
VALUES
    (current_timestamp, '329551a1-215d-43e6-baee-322f2467272d', 
    'Desktop', 1, 1000.00, null, 1000.00);

-- Проверка вставленных данных
SELECT * FROM orders
WHERE user_id = '329551a1-215d-43e6-baee-322f2467272d'
ORDER BY order_id DESC;

-- Откат изменений
ROLLBACK;




-- Задание 2
/*
Клиенты сервиса в свой день рождения получают скидку. 
Расчёт скидки и отправка клиентам промокодов происходит на стороне сервера приложения. 
Каждый раз список именинников формируется и возвращается недостаточно быстро. 
Оптимизируйте этот процесс.
*/
--Список клиентов возвращается из БД в приложение таким запросом:
--Анализируем запрос
EXPLAIN ANALYSE
SELECT user_id::text::uuid, first_name::text, last_name::text, 
    city_id::bigint, gender::text
FROM users
WHERE city_id::integer = 4
    AND date_part('day', to_date(birth_date::text, 'yyyy-mm-dd')) 
        = date_part('day', to_date('31-12-2023', 'dd-mm-yyyy'))
    AND date_part('month', to_date(birth_date::text, 'yyyy-mm-dd')) 
        = date_part('month', to_date('31-12-2023', 'dd-mm-yyyy'));


----- РЕШЕНИЕ -----
-- Проблемы:
/* 1. Использование функций в условиях:
Преобразование birth_date в строку и использование функций to_date и date_part 
делает невозможным использование индексов. 
Это приводит к полному сканированию таблицы (вместо использования индексов), что значительно замедляет запрос.
2. Типы данных:
Почти все колонки таблицы хранится как строка в формате character(500), что увеличивает накладные расходы на преобразования.
*/
-- Решение
/* 
1. Изменим типы данных столбцов, что позволит избежать лишних преобразований строк в каждом запросе.
2. Добавление индекса на колонку city_id, чтобы ускорить фильтрацию.
3. Перепишем запрос.
*/

BEGIN;

ALTER TABLE users
ALTER COLUMN birth_date TYPE DATE 
USING birth_date::date;

ALTER TABLE users
ALTER COLUMN first_name TYPE TEXT 
USING first_name::text; 

ALTER TABLE users
ALTER COLUMN last_name TYPE TEXT 
USING last_name::text; 

ALTER TABLE users
ALTER COLUMN gender TYPE TEXT 
USING gender::text; 

ALTER TABLE users
ALTER COLUMN city_id TYPE INTEGER 
USING city_id::integer; 

ALTER TABLE users
ALTER COLUMN user_id TYPE UUID 
USING user_id::text::uuid; 

CREATE INDEX users_city_id_idx ON public.users ( city_id);


EXPLAIN ANALYSE
SELECT user_id, first_name, last_name, 
    city_id, gender
FROM users
WHERE city_id = 4
AND to_char(birth_date, 'MM-DD') = '12-31';

ROLLBACK;


-- Задание 3
/*
Также пользователи жалуются, что оплата при оформлении заказа проходит долго.
Разработчик сервера приложения проанализировал ситуацию и заключил, 
что оплата «висит» из-за того, что выполнение процедуры add_payment 
требует довольно много времени по меркам БД. 
Найдите в базе данных эту процедуру и подумайте, 
как можно ускорить её работу.
*/
CREATE OR REPLACE PROCEDURE public.add_payment(
	IN p_order_id bigint,
	IN p_sum_payment numeric)
LANGUAGE 'plpgsql'
AS $BODY$
BEGIN
    INSERT INTO order_statuses (order_id, status_id, status_dt)
    VALUES (p_order_id, 2, statement_timestamp());
    
    INSERT INTO payments (payment_id, order_id, payment_sum)
    VALUES (nextval('payments_payment_id_sq'), p_order_id, p_sum_payment);
    
    INSERT INTO sales(sale_id, sale_dt, user_id, sale_sum)
    SELECT NEXTVAL('sales_sale_id_sq'), statement_timestamp(), user_id, p_sum_payment
    FROM orders WHERE order_id = p_order_id;
END;
$BODY$;

----- РЕШЕНИЕ -----
--Упростим бизнес-логику. Применим подход денормализации
--Имеет смысл объединить таблицы payments и sales, 
--добавив order_id в sales и удалив таблицу payments. 
--Это может упростить логику приложения, 
--уменьшить накладные расходы на операции и ускорить обработку данных.

--Добавим поле order_id в таблицу sales:
ALTER TABLE sales ADD COLUMN order_id BIGINT;

--Переносим данные из таблицы payments в sales:
INSERT INTO sales (sale_id, sale_dt, user_id, sale_sum, order_id)
SELECT 
    nextval('sales_sale_id_sq'), 
    CURRENT_TIMESTAMP,  -- Или другое подходящее значение
    o.user_id, 
    p.payment_sum, 
    p.order_id
FROM payments p
JOIN orders o ON o.order_id = p.order_id;

--Удаление таблицы payments:
DROP TABLE payments;



--Обновление процедуры add_payment: 
CREATE OR REPLACE PROCEDURE public.add_payment(
    IN p_order_id BIGINT,
    IN p_sum_payment NUMERIC
)
LANGUAGE 'plpgsql'
AS $BODY$
BEGIN
    INSERT INTO order_statuses (order_id, status_id, status_dt)
    VALUES (p_order_id, 2, statement_timestamp());
    
    INSERT INTO sales (sale_id, sale_dt, user_id, sale_sum, order_id)
    SELECT 
        nextval('sales_sale_id_sq'),statement_timestamp(), user_id, 
        p_sum_payment, p_order_id
    FROM orders WHERE order_id = p_order_id
    LIMIT 1;
END;
$BODY$;

CALL add_payment(100001, 1000000.87);




-- Задание 4
/* 
Все действия пользователей в системе логируются и записываются в таблицу user_logs. 
Потом эти данные используются для анализа — как правило, анализируются данные за текущий квартал.
Время записи данных в эту таблицу сильно увеличилось, а это тормозит практически все действия пользователя. 
Подумайте, как можно ускорить запись. Вы можете сдать решение этой задачи без скрипта или — попробовать написать скрипт. 
*/

-- РЕШЕНИЕ
/*Данные в user_logs, относящиеся к текущему кварталу, активно используются для анализа, 
а старые данные могут быть менее востребованы. 
Следует рассмотреть подход с разделением на партиции.
Партиционирование таблицы по log_date.
Партиционирование позволяет распределять данные по подтаблицам, 
повышая производительность записи и запросов.
*/

--SELECT distinct extract(year from log_date)  FROM user_logs;

-- Создаём главную таблицу
CREATE TABLE public.user_logs_main (
    visitor_uuid character varying(128) COLLATE pg_catalog."default",
    user_id uuid,
    event character varying(128) COLLATE pg_catalog."default",
    datetime timestamp without time zone,
    log_date date NOT NULL,
    log_id bigint NOT NULL DEFAULT nextval('user_logs_log_id_seq'::regclass),
    CONSTRAINT user_logs_pkey PRIMARY KEY (log_id, log_date)
) PARTITION BY RANGE (log_date);

-- Создаём партиции для текущего и прошлых кварталов
CREATE TABLE public.user_logs_q1_21 PARTITION OF public.user_logs_main
    FOR VALUES FROM ('2021-01-01') TO ('2021-04-01');

CREATE TABLE public.user_logs_q2_21 PARTITION OF public.user_logs_main
    FOR VALUES FROM ('2021-04-01') TO ('2021-07-01');

CREATE TABLE public.user_logs_q3_21 PARTITION OF public.user_logs_main
    FOR VALUES FROM ('2021-07-01') TO ('2021-10-01');

CREATE TABLE public.user_logs_q4_21 PARTITION OF public.user_logs_main
    FOR VALUES FROM ('2021-10-01') TO ('2022-01-01');
--- ... ---
/*Новые записи будут записываться только в активную партицию текущего квартала.
Анализ будет выполняться быстрее, так как запросы можно ограничить только нужной частью данных.*/



-- Задание 5
/* Маркетологи сервиса регулярно анализируют предпочтения 
различных возрастных групп. Для этого они формируют отчёт:
day	age	spicy	fish	meat
	0–20			
	20–30			
	30–40			
	40–100			
В столбцах spicy, fish и meat отображается, какой % блюд, 
заказанных каждой категорией пользователей, содержал эти признаки.
В возрастных интервалах верхний предел входит в интервал, а нижний — нет.
Также по правилам построения отчётов в них не включается текущий день.
Администратор БД Серёжа заметил, что регулярные похожие запросы 
от разных маркетологов нагружают базу, и в результате увеличивается время работы приложения.
Подумайте с точки зрения производительности, как можно 
оптимально собирать и хранить данные для такого отчёта. 
В ответе на это задание не пишите причину — просто опишите ваш 
способ получения отчёта и добавьте соответствующий скрипт.
*/
ALTER TABLE users
ALTER COLUMN user_id TYPE UUID 
USING user_id::text::uuid; 


-- Создание материализованного представления
CREATE MATERIALIZED VIEW public.user_order_preferences AS
SELECT
	--od.order_dt::date,
    CURRENT_DATE - 1 AS report_date, -- Отчёт за предыдущий день
    CASE
        WHEN age < 20 THEN '0–20'
        WHEN age >= 20 AND age < 30 THEN '20–30'
        WHEN age >= 30 AND age < 40 THEN '30–40'
        ELSE '40–100'
    END AS age_group,
    ROUND(SUM(CASE WHEN d.spicy = 1 THEN 1 ELSE 0 END)::numeric / COUNT(*) * 100, 2) AS spicy_percent,
    ROUND(SUM(CASE WHEN d.fish = 1 THEN 1 ELSE 0 END)::numeric / COUNT(*) * 100, 2) AS fish_percent,
    ROUND(SUM(CASE WHEN d.meat = 1 THEN 1 ELSE 0 END)::numeric / COUNT(*) * 100, 2) AS meat_percent

FROM
    orders od
    JOIN order_items oi ON od.order_id = oi.order_id
    JOIN dishes d ON oi.item = d.object_id
    JOIN users u ON u.user_id = od.user_id
    CROSS JOIN LATERAL (
        SELECT EXTRACT(YEAR FROM CURRENT_DATE) - EXTRACT(YEAR FROM u.birth_date) AS age
    ) AS user_age
WHERE
	od.order_dt < CURRENT_DATE
GROUP BY
    age_group
	--, od.order_dt::date
ORDER BY age_group;

-- Добавление индекса для ускорения запросов к представлению
CREATE INDEX idx_user_order_preferences_age_group
ON public.user_order_preferences (age_group);

-- Обновление материализованного представления
-- Это можно выполнять вручную или через планировщик задач
REFRESH MATERIALIZED VIEW public.user_order_preferences;

