SELECT pg_stat_statements_reset();

-- 1
-- вычисляет среднюю стоимость блюда в определенном ресторане
SELECT avg(dp.price)
FROM dishes_prices dp
    JOIN dishes d ON dp.dishes_id = d.object_id
WHERE d.rest_id LIKE '%14ce5c408d2142f6bd5b7afad906bc7e%'
	AND dp.date_begin::date <= current_date
	AND (dp.date_end::date >= current_date
		OR dp.date_end IS NULL);
	
-- 2
-- выводит данные о конкретном заказе: id, дату, стоимость и текущий статус
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
   
-- 3
-- выводит id и имена пользователей, фамилии которых входят в список
SELECT u.user_id, u.first_name
FROM users u
WHERE u.last_name IN ('КЕДРИНА', 'АДОА', 'АКСЕНОВА', 'АЙМАРДАНОВА', 'БОРЗЕНКОВА', 'ГРИПЕНКО', 'ГУЦА'
                     , 'ЯВОРЧУКА', 'ХВИЛИНА', 'ШЕЙНОГА', 'ХАМЧИЧЕВА', 'БУХТУЕВА', 'МАЛАХОВЦЕВА', 'КРИСС'
                     , 'АЧАСОВА', 'ИЛЛАРИОНОВА', 'ЖЕЛЯБИНА', 'СВЕТОЗАРОВА', 'ИНЖИНОВА', 'СЕРДЮКОВА', 'ДАНСКИХА')
ORDER BY 1 DESC;

-- 4
-- ищет все салаты в списке блюд
SELECT d.object_id, d.name
FROM dishes d
WHERE d.name LIKE 'salat%';

-- 5
-- определяет максимальную и минимальную сумму заказа по городу
SELECT max(p.payment_sum) max_payment, min(p.payment_sum) min_payment
FROM payments p
    JOIN orders o ON o.order_id = p.order_id
WHERE o.city_id = 2;

-- 6
-- ищет всех партнеров определенного типа в определенном городе
SELECT p.id partner_id, p.chain partner_name
FROM partners p
    JOIN cities c ON c.city_id = p.city_id
WHERE p.type = 'Пекарня'
	AND c.city_name = 'Владивосток';

-- 7
-- ищет действия и время действия определенного посетителя
SELECT event, datetime
FROM user_logs
WHERE visitor_uuid = 'fd3a4daa-494a-423a-a9d1-03fd4a7f94e0'
ORDER BY 2;

-- 8
-- ищет логи за текущий день
SELECT *
FROM user_logs
WHERE datetime::date > current_date;

-- 9
-- определяет количество неоплаченных заказов
SELECT count(*)
FROM order_statuses os
    JOIN orders o ON o.order_id = os.order_id
WHERE (SELECT count(*)
	   FROM order_statuses os1
	   WHERE os1.order_id = o.order_id AND os1.status_id = 2) = 0
	AND o.city_id = 1;

-- 10
-- определяет долю блюд дороже 1000
SELECT (SELECT count(*)
	    FROM dishes_prices dp
	    WHERE dp.date_end IS NULL
		    AND dp.price > 1000.00)::NUMERIC / count(*)::NUMERIC
FROM dishes_prices
WHERE date_end IS NULL;

-- 11
-- отбирает пользователей определенного города, чей день рождения находится в интервале +- 3 дня от текущей даты
SELECT user_id, current_date - birth_date
FROM users
WHERE city_id = 1
	AND birth_date >= current_date - 3
	AND birth_date <= current_date + 3;

-- 12
-- вычисляет среднюю стоимость блюд разных категорий
SELECT 'average price with fish', avg(dp.price)
FROM dishes_prices dp
    JOIN dishes d ON d.object_id = dp.dishes_id
WHERE dp.date_end IS NULL AND d.fish = 1
UNION
SELECT 'average price with meat', avg(dp.price)
FROM dishes_prices dp
    JOIN dishes d ON d.object_id = dp.dishes_id
WHERE dp.date_end IS NULL AND d.meat = 1
UNION
SELECT 'average price of spicy food', avg(dp.price)
FROM dishes_prices dp
    JOIN dishes d ON d.object_id = dp.dishes_id
WHERE dp.date_end IS NULL AND d.spicy = 1
ORDER BY 2;

-- 13
-- ранжирует города по общим продажам за определенный период
SELECT ROW_NUMBER() OVER( ORDER BY sum(o.final_cost) DESC),
	c.city_name,
	sum(o.final_cost)
FROM cities c
    JOIN orders o ON o.city_id = c.city_id
WHERE order_dt >= to_timestamp('01.01.2021 00-00-00', 'dd.mm.yyyy hh24-mi-ss')
	AND order_dt < to_timestamp('02.01.2021', 'dd.mm.yyyy hh24-mi-ss')
GROUP BY c.city_name;

-- 14
-- вычисляет количество заказов определенного пользователя
SELECT COUNT(*)
FROM orders
WHERE user_id = '0fd37c93-5931-4754-a33b-464890c22689';

-- 15
-- вычисляет количество заказов позиций, продажи которых выше среднего
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