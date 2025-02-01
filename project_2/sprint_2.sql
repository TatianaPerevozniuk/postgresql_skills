----- ЭТАП 1 -----
-- Создадим таблицы с продвинутыми типами данных --

-- 1. Cоздадим enum cafe.restaurant_type с типом заведения
CREATE TYPE cafe.restaurant_type AS ENUM ('coffee_shop', 'restaurant', 'bar', 'pizzeria');

-- 2. Создадтм таблицу cafe.restaurants с информацией о ресторанах
CREATE TABLE cafe.restaurants (
	restaurant_uuid uuid PRIMARY KEY DEFAULT GEN_RANDOM_UUID(),
	cafe_name VARCHAR(50) NOT NULL,
	locations geometry(POINT, 4326) NOT NULL,
	type cafe.restaurant_type NOT NULL,
	menu jsonb NOT NULL
);

INSERT INTO cafe.restaurants (cafe_name, locations, type, menu)
SELECT DISTINCT cafe_name, ST_SetSRID(ST_MakePoint(longitude, latitude), 4326) AS locations, type::cafe.restaurant_type, menu 
FROM raw_data.sales
JOIN raw_data.menu USING(cafe_name)
RETURNING cafe.restaurants.*;

SELECT * FROM cafe.restaurants;


-- 3. Создадим таблицу cafe.managers с информацией о менеджерах. 
CREATE TABLE cafe.managers (
	manager_uuid uuid PRIMARY KEY DEFAULT GEN_RANDOM_UUID(),
	manager VARCHAR(50) NOT NULL, 
	manager_phone VARCHAR(50) NOT NULL
);

INSERT INTO cafe.managers (manager, manager_phone)
SELECT DISTINCT manager, manager_phone
FROM raw_data.sales
RETURNING cafe.managers.*;

-- 4. Создим таблицу cafe.restaurant_manager_work_dates. 
--Зададим составной первичный ключ из двух полей: restaurant_uuid и manager_uuid. 
--Работа менеджера в ресторане от даты начала до даты окончания — единый период, без перерывов.

CREATE TABLE cafe.restaurant_manager_work_dates (
	 restaurant_uuid UUID,
     manager_uuid UUID,
	 date_from DATE, --дата начала работы в ресторане
	 date_to DATE, --дата окончания работы
     PRIMARY KEY (restaurant_uuid, manager_uuid)
);

INSERT INTO cafe.restaurant_manager_work_dates (restaurant_uuid, manager_uuid, date_from, date_to)
SELECT restaurant_uuid, manager_uuid, MIN(report_date) date_from, MAX(report_date) date_to
FROM raw_data.sales
JOIN cafe.restaurants USING(cafe_name)
NATURAL JOIN cafe.managers
GROUP BY restaurant_uuid, manager_uuid
RETURNING cafe.restaurant_manager_work_dates.*;

-- 5. Создадим таблицу cafe.sales со столбцами: date, restaurant_uuid, avg_check. 
--Зададим составной первичный ключ из даты и uuid ресторана.

CREATE TABLE cafe.sales (
	report_date DATE,
	restaurant_uuid UUID,
	avg_check NUMERIC(6,2) CONSTRAINT avg_check_positive CHECK (avg_check > 0) NOT NULL,
	PRIMARY KEY (report_date, restaurant_uuid)
);

INSERT INTO cafe.sales (report_date, restaurant_uuid, avg_check)
SELECT report_date, restaurant_uuid, avg_check
FROM raw_data.sales
JOIN cafe.restaurants USING(cafe_name);

SELECT * FROM cafe.sales
LIMIT 100;


----- ЭТАП 2 -----
-- Создадим представления и напишем аналитические запросы --

-- Задание 1
-- Чтобы выдать премию менеджерам, нужно понять, у каких заведений самый высокий средний чек. 
-- Создадим представление, которое покажет топ-3 заведений внутри каждого типа заведения по среднему чеку за все даты. 

CREATE VIEW cafe.v_top_cafe AS
WITH top AS (
	SELECT cafe_name, 
	   "type", 
	   ROUND(AVG(avg_check), 2) avg_ch, 
	   ROW_NUMBER() OVER(PARTITION BY "type" ORDER BY AVG(avg_check) DESC) rn
	FROM cafe.sales s
	JOIN cafe.restaurants r USING(restaurant_uuid)
	GROUP BY cafe_name, "type"
)
SELECT cafe_name, "type", avg_ch FROM top
WHERE rn < 4;

SELECT * FROM cafe.v_top_cafe;

-- DROP VIEW cafe.v_top_cafe;

-- Задание 2
-- Создадим материализованное представление, которое покажет, как изменяется средний чек для каждого заведения 
-- от года к году за все года за исключением текущего года. 

-- DROP MATERIALIZED VIEW cafe.mv_check_change;
CREATE MATERIALIZED VIEW cafe.mv_check_change AS
WITH av AS (
	SELECT EXTRACT(YEAR FROM report_date) "year", restaurant_uuid, 
		   ROUND(AVG(avg_check), 2) avg_check_this_year,
		   ROUND(LAG(AVG(avg_check), 1, 0) OVER(PARTITION BY restaurant_uuid ORDER BY EXTRACT(YEAR FROM report_date)), 2) avg_check_last_year
	FROM cafe.sales
	WHERE EXTRACT(YEAR FROM report_date) <> EXTRACT(YEAR FROM CURRENT_DATE)
	GROUP BY EXTRACT(YEAR FROM report_date), restaurant_uuid
	ORDER BY restaurant_uuid, 1
)
SELECT av.year, 
	   r.cafe_name, 
	   r.type, 
	   av.avg_check_this_year,
	   av.avg_check_last_year,
	   CASE 
	   		WHEN av.avg_check_last_year = 0 THEN 0
			ELSE ROUND((av.avg_check_this_year/av.avg_check_last_year - 1) * 100, 2)
			END avg_check_change_pct
FROM av
JOIN cafe.restaurants r USING(restaurant_uuid)
ORDER BY 3, 2, 1;

SELECT * FROM cafe.mv_check_change;
-- REFRESH MATERIALIZED VIEW cafe.mv_check_change;


-- Задание 3
-- Найдем топ-3 заведения, где чаще всего менялся менеджер за весь период.

WITH c AS (
	SELECT cafe_name, COUNT(DISTINCT manager_uuid) count_manager,
		DENSE_RANK() OVER(ORDER BY COUNT(manager_uuid) DESC) dr
	FROM cafe.restaurant_manager_work_dates mw
	JOIN cafe.restaurants r USING(restaurant_uuid)
	GROUP BY cafe_name
)
SELECT cafe_name, count_manager
FROM c
WHERE dr < 4;


-- Задание 4
-- Найдем пиццерию с самым большим количеством пицц в меню. Если таких пиццерий несколько, выведем все.
WITH max_piz AS (
	SELECT cafe_name, COUNT(*) AS pizza_count,
	   DENSE_RANK() OVER(ORDER BY COUNT(*) DESC) dr
	FROM cafe.restaurants, jsonb_each(menu->'Пицца')
	WHERE "type" = 'pizzeria'
	GROUP BY cafe_name
)
SELECT cafe_name, pizza_count FROM max_piz
WHERE dr = 1;


-- Задание 5
-- Найдем самую дорогую пиццу для каждой пиццерии.
WITH max_price AS (
	SELECT cafe_name, (SELECT jsonb_object_keys(menu) LIMIT 1) AS food_type, pizza_name, pizza_price::numeric,
	   ROW_NUMBER() OVER (PARTITION BY cafe_name ORDER BY pizza_price::numeric DESC) AS rn
	FROM cafe.restaurants, jsonb_each(menu->'Пицца') AS p(pizza_name, pizza_price)
	WHERE "type" = 'pizzeria'
)
SELECT cafe_name, food_type, pizza_name, pizza_price FROM max_price
WHERE rn = 1;


-- Задание 6
-- Найдем два самых близких друг к другу заведения одного типа.

SELECT r.cafe_name, j.cafe_name, "type", 
	   ROUND(ST_Distance(r.locations::geography, j.locations::geography)) distance
FROM cafe.restaurants r
JOIN cafe.restaurants j USING("type")
WHERE r.cafe_name <> j.cafe_name
ORDER BY distance 
LIMIT 1;


-- Задание 7
-- Найдем район с самым большим количеством заведений и район с самым маленьким количеством заведений. 
-- Первой строчкой выведем район с самым большим количеством заведений, второй — с самым маленьким. 

WITH count_cafe AS (
	SELECT d.district_name, COUNT(r.restaurant_uuid) count_restaurants
	FROM cafe.restaurants r
	JOIN cafe.districts d ON ST_Intersects(r.locations, d.district_geom)
	GROUP BY d.district_name
	ORDER BY 1
)
SELECT * FROM (SELECT district_name, count_restaurants FROM count_cafe
			   WHERE count_restaurants = (SELECT MAX(count_restaurants) FROM count_cafe)
			   UNION
			   SELECT district_name, count_restaurants FROM count_cafe
			   WHERE count_restaurants = (SELECT MIN(count_restaurants) FROM count_cafe)
			   ) count_cafe_max_min
ORDER BY 2 DESC;




