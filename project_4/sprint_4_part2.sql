-------- ЧАСТЬ 2 --------
-- ОПТИМИЗАЦИЯ ЗАПРОСОВ --
--В файле user_scripts_pr4.sql — пользовательские скрипты, которые выполняются на базе данных. 
--Выполним их на своём компьютере. Проверим, что в нашей СУБД включён модуль pg_stat_statements 
--Проверим установлен ли расширение pg_stat_statements в текущей базе данных:
SELECT * 
FROM pg_available_extensions 
WHERE name = 'pg_stat_statements';
--подключено ли расширение:
SELECT * 
FROM pg_extension 
WHERE extname = 'pg_stat_statements';
--активирован ли модуль в конфигурации PostgreSQL:
SHOW shared_preload_libraries;
--включена ли сборка статистики запросов: (all/top)
SHOW pg_stat_statements.track;

----- ЗАДАЧА -----
-- Наша задача — найти пять самых медленных скриптов и оптимизировать их. 
-- Важно: при оптимизации в этой части проекта нельзя менять структуру БД.

--Сброс статистики
SELECT pg_stat_statements_reset();
--Запуск скриптов из файла user_scripts_pr4.sql



-- Ищем медленные запросы в базе данных --

-- Найдем идентификатор своей базы данных:
SELECT oid, datname FROM pg_database; --наш oid = 33914

-- Найдем пять самых медленных запросов: 
-- отсортируем вывод представления pg_stat_statements по полю total_exec_time (общее время, потраченное на выполнение запроса) 
-- или mean_exec_time (среднее время):
SELECT  
    query,
    ROUND(mean_exec_time::numeric,2) mean_exec_time,                
    ROUND(total_exec_time::numeric,2) total_exec_time,
    ROUND(min_exec_time::numeric,2) min_exec_time, 
    ROUND(max_exec_time::numeric,2) max_exec_time,
    calls,
    rows                          
FROM pg_stat_statements
-- Подставьте своё значение dbid.
WHERE dbid = 33914 
ORDER BY mean_exec_time DESC
LIMIT 5; 


----- ЗАПРОС 1 -----
-- Анализируем самый медленный запрос
--Чтобы выполнить запрос и получить фактический план с реальными затратами, 
--используем команду EXPLAIN с параметром ANALYZE:

-- 9
-- определяет количество неоплаченных заказов
EXPLAIN ANALYZE
SELECT count(*)
FROM order_statuses os
    JOIN orders o ON o.order_id = os.order_id
WHERE (SELECT count(*)
	   FROM order_statuses os1
	   WHERE os1.order_id = o.order_id AND os1.status_id = 2) = 0
	AND o.city_id = 1;

--Общее время выполнения скрипта до оптимизации: 32961 ms.

--Узлы с высокой стоимостью:
--	1. Подзапрос SubPlan 1, содержащий Seq Scan (последовательное сканирование) по order_statuses для каждого order_id.
--	2. Nested Loop, выполняющийся для каждой строки из order_statuses.

--Проблемы
--	1. Используется коррелированный подзапрос, который выполняется для каждой строки, что увеличивает нагрузку.
--	2. Нет индекса для ускорения фильтрации по полям order_statuses.order_id и order_statuses.status_id.
--	3. Последовательное сканирование (Seq Scan) увеличивает время выполнения.

--Шаги для оптимизации
--	1. Убрать коррелированный подзапрос.
--	2. Добавить индексы на ключевые поля:
--		- order_statuses(order_id, status_id) для фильтрации по status_id.
--		- Проверить наличие индекса orders(city_id).

--Оптимизированный запрос:
EXPLAIN ANALYZE
SELECT COUNT(*)
FROM orders o
WHERE o.city_id = 1
  AND NOT EXISTS (
    SELECT 1
    FROM order_statuses os
    WHERE os.order_id = o.order_id
      AND os.status_id = 2
  );

--Выполним команды создания индексов:
CREATE INDEX order_statuses_order_id_status_id_idx ON order_statuses(order_id, status_id);
CREATE INDEX orders_city_id_idx ON orders(city_id);

-- Выполним еще раз запрос с анализом:
EXPLAIN ANALYZE
SELECT COUNT(*)
FROM orders o
WHERE o.city_id = 1
  AND NOT EXISTS (
    SELECT 1
    FROM order_statuses os
    WHERE os.order_id = o.order_id
      AND os.status_id = 2
  );

--После оптимизации:
--	- Устранили коррелированный подзапрос, замена на Hash Right Anti Join.
--	- Использование индекса orders_city_id_idx и агрегации.
--	- Время выполнения: 12.357 ms.
--	- Снижение затрат выполнения более чем в 1,250 раз.



----- ЗАПРОС 2 -----
-- 8
-- ищет логи за текущий день
EXPLAIN ANALYZE
SELECT *
FROM user_logs
WHERE datetime::date > current_date;

--Общее время выполнения скрипта до оптимизации: 1075.915 ms.

-- Узлы с высокой стоимостью:
--	1. Append (стоимость: 156011.42)
--		Операция объединения данных из всех секций таблицы user_logs.
--		Высокая стоимость обусловлена тем, что каждая секция таблицы сканируется полностью, несмотря на то, что значительная часть данных отфильтровывается.
--	2. Seq Scan на каждой секции (user_logs_1, user_logs_y2021q2, user_logs_y2021q3, user_logs_y2021q4)

-- Основные проблемы:
--	- Приведение типов datetime::date делает невозможным использование индексов.
--	- Каждая секция таблицы сканируется полностью, что увеличивает нагрузку.
--	- Большое количество удаляемых строк, так как фильтр применяется ко всем данным.

--Оптимизированный запрос:
EXPLAIN ANALYZE
SELECT *
FROM user_logs
WHERE datetime >= CURRENT_DATE;

--Условие datetime >= CURRENT_DATE заменяет datetime::date > CURRENT_DATE, позволяя эффективно использовать индексы.
--Время выполнения: 0.058 ms (уменьшено более чем в 18000 раз).
--Стоимость выполнения снижена с 156011.42 до 37.38.




----- ЗАПРОС 3 -----
-- 7
-- ищет действия и время действия определенного посетителя
EXPLAIN ANALYZE
SELECT event, datetime
FROM user_logs
WHERE visitor_uuid = 'fd3a4daa-494a-423a-a9d1-03fd4a7f94e0'
ORDER BY 2;

--Общее время выполнения скрипта до оптимизации: 210.294 ms.

-- Узлы с высокой стоимостью:
--	1. Parallel Append (cost=0.00..91116.21):
-- 	- Производится последовательное параллельное сканирование секций user_logs, что дорого с точки зрения вычислений.
-- 	- Используются параллельные последовательные сканирования (Parallel Seq Scan), которые проверяют строки на соответствие условию visitor_uuid = 'fd3a4daa-494a-423a-a9d1-03fd4a7f94e0'.
-- 	- Большое количество строк отфильтровывается:
-- 		user_logs_1: удалено 615,119 строк.
-- 		user_logs_y2021q2: удалено 1,132,470 строк.
-- 		user_logs_y2021q3: удалено 25,304 строк.
-- 		user_logs_y2021q4: удалено 0 строк.
-- 	2. Sort (cost=91119.53..91119.78):
-- 	- Сортировка данных по datetime.
-- 	- Метод быстрой сортировки (quicksort) использует до 25 кБ памяти на поток.

-- Основные проблемы:
-- 	Отсутствие индекса по visitor_uuid:
-- 		Это приводит к полному сканированию всех секций таблицы.
-- 		Запрос проверяет миллионы строк, из которых лишь несколько соответствуют условию.


--Оптимизированный запрос:
-- Создадим индекс для ускорения поиска по visitor_uuid 
-- и одновременно включим сортировку по datetime, 
-- чтобы избежать отдельной операции сортировки.
CREATE INDEX user_logs_visitor_uuid_datetime_idx ON user_logs (visitor_uuid, datetime);
CREATE INDEX user_logs_y2021q2_visitor_uuid_datetime_idx ON user_logs_y2021q2 (visitor_uuid, datetime);
CREATE INDEX user_logs_y2021q3_visitor_uuid_datetime_idx ON user_logs_y2021q3 (visitor_uuid, datetime);
CREATE INDEX user_logs_y2021q4_visitor_uuid_datetime_idx ON user_logs_y2021q4 (visitor_uuid, datetime);

--Запустим скрипт
EXPLAIN ANALYZE
SELECT event, datetime
FROM user_logs
WHERE visitor_uuid = 'fd3a4daa-494a-423a-a9d1-03fd4a7f94e0'
ORDER BY datetime;

--Время выполнения после оптимизации: 0.090 ms
--Вывод:
-- Нет полного сканирования секций таблиц, используются эффективные операции Bitmap Heap Scan и Index Scan.
-- Теперь условие фильтрации выполняется на уровне индекса, что устраняет необходимость сканирования большого количества строк.
-- Оптимизация дала значительное улучшение производительности, запрос теперь мгновенный.




----- ЗАПРОС 4 -----
-- 2
-- выводит данные о конкретном заказе: id, дату, стоимость и текущий статус
EXPLAIN ANALYZE
SELECT o.order_id, o.order_dt, o.final_cost, s.status_name
FROM order_statuses os
    JOIN orders o ON o.order_id = os.order_id
    JOIN statuses s ON s.status_id = os.status_id
WHERE o.user_id = 'c2885b45-dddd-4df3-b9b3-2cc012df727c'::uuid
	AND os.status_dt IN (
	SELECT max(status_dt)
	FROM order_statuses
	WHERE order_id = o.order_id
    );

--Общее время выполнения скрипта до оптимизации: 364,414 мс.

--Оптимизация запроса:
--Индекс orders.user_id уже существует.
--Заменим сравнение с in на inner join. И перепишем запрос с CTE:

EXPLAIN ANALYZE
WITH latest_statuses AS (
    SELECT 
        order_id,
        MAX(status_dt) AS latest_status_dt
    FROM order_statuses
    GROUP BY order_id
)
SELECT 
    o.order_id,
    o.order_dt,
    o.final_cost,
    s.status_name
FROM orders o
JOIN order_statuses os
    ON o.order_id = os.order_id
JOIN statuses s
    ON os.status_id = s.status_id
JOIN latest_statuses ls
    ON os.order_id = ls.order_id AND os.status_dt = ls.latest_status_dt
WHERE o.user_id = 'c2885b45-dddd-4df3-b9b3-2cc012df727c'::uuid;

--Время выполнения запроса после оптимизации: 92.966 мс.
--Вывод:
-- Улучшена читаемость и поддерживаемость запроса за счет использования CTE.




----- ЗАПРОС 5 -----
-- 15
-- вычисляет количество заказов позиций, продажи которых выше среднего
EXPLAIN ANALYZE
SELECT d.name, SUM(count) AS orders_quantity
FROM order_items oi
JOIN dishes d ON d.object_id = oi.item
WHERE oi.item IN (
	SELECT item
	FROM (SELECT item, SUM(count) AS total_sales
		  FROM order_items oi
		  GROUP BY 1) dishes_sales
	WHERE dishes_sales.total_sales > (
		SELECT SUM(t.total_sales)/ COUNT(*)
		FROM (SELECT item, SUM(count) AS total_sales
			FROM order_items oi
			GROUP BY
				1) t)
)
GROUP BY 1
ORDER BY orders_quantity DESC;

-- Общее время выполнения скрипта до оптимизации: 105.612 ms

-- Узлы с высокой стоимостью:
--Hash Join (стоимость=1522,66–3147,65): со временем выполнения 59,041–79,705 мс.
--HashAggregate(стоимость=1480,72–1492,13): со временем выполнения 58,455–58,635 мс.
--Последовательное сканирование (Seq Scan) на order_items (стоимость=0,00..1134,48): время выполнения 6,338 мс.

-- Основные проблемы:
-- Повторяющиеся операции: запрос дважды вычисляет агрегаты (SUM и COUNT) для order_items. Это приводит к увеличению затрат.
-- Использование подзапросов в WHERE: Подзапрос для вычисления среднего суммарного количества продаж вызывает дополнительные проходы по данным.
-- Последовательное сканирование (Seq Scan): отсутствие индексации часто используемых столбцов (item и object_id) приводит к полному сканированию таблиц.
-- Недостаток индексов: на ключевых столбцах, таких как order_items.item и dishes.object_id.


-- Оптимизированный запрос:
--Создатдим индексы для ускорения поиска и соединений:
CREATE INDEX order_items_item_idx ON order_items(item);
CREATE INDEX dishes_object_id_idx ON dishes(object_id);
-- Оптимизация выполняется за счет:
-- Исключения подзапросов.
-- Сокращения количества операций агрегации.
-- Использования CTE (Common Table Expressions) для читаемости.

EXPLAIN ANALYZE
WITH aggregated_sales AS (
    SELECT 
        item, 
        SUM(count) AS total_sales
    FROM order_items
    GROUP BY item
),
average_sales AS (
    SELECT 
        SUM(total_sales) / COUNT(*) AS avg_sales
    FROM aggregated_sales
)
SELECT 
    d.name, 
    SUM(oi.count) AS orders_quantity
FROM order_items oi
JOIN dishes d ON d.object_id = oi.item
JOIN aggregated_sales ags ON ags.item = oi.item
CROSS JOIN average_sales
WHERE ags.total_sales > average_sales.avg_sales
GROUP BY d.name
ORDER BY orders_quantity DESC;


--Время выполнения после оптимизации: 66.890 ms
--Вывод:
-- Устранены повторяющиеся подзапросы и лишние агрегации.
-- Добавлены индексы для ускорения соединений и фильтрации.
-- Улучшена читаемость и поддерживаемость запроса за счет использования CTE.





