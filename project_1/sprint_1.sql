--------- ЭТАП 1 ----------------
---Создаем БД sprint_1
CREATE DATABASE sprint_1;

--Создадим схему и таблицу для загрузки сырых данных
CREATE SCHEMA raw_data;

CREATE TABLE raw_data.sales (
	id INT PRIMARY KEY,
	auto VARCHAR(255),
	gasoline_consumption NUMERIC(3,1) DEFAULT NULL,
	price NUMERIC(10,3),
	date DATE,
	person VARCHAR(255),
	phone VARCHAR(255),
	discount INT,
	brand_origin VARCHAR(255)
);

--Импорт сырых данных:
COPY raw_data.sales(id, auto, gasoline_consumption, price, date, person, phone, discount, brand_origin)
FROM '/Users/cars.csv' CSV HEADER NULL 'null'; 

SELECT * FROM raw_data.sales;

UPDATE raw_data.sales
SET brand_origin = 'Germany'
WHERE auto ILIKE ('%Porsche%')
RETURNING raw_data.sales.*;


--Создаем схему car_shop, а в ней создаем нормализованные таблицы (до третьей нормальной формы).
CREATE SCHEMA car_shop;
---------


CREATE TABLE car_shop.country (
	country_id SERIAL PRIMARY KEY,
	brand_origin VARCHAR(50) NOT NULL
);
INSERT INTO car_shop.country (brand_origin)
	SELECT DISTINCT brand_origin 
	FROM raw_data.sales
	ORDER BY 1
RETURNING car_shop.country.*;
--------------------------

CREATE TABLE car_shop.brand(
	brand_id SERIAL PRIMARY KEY,
	brand_name VARCHAR(50) NOT NULL UNIQUE,
	country_id SERIAL REFERENCES car_shop.country (country_id)
);
INSERT INTO car_shop.brand (brand_name, country_id)
	SELECT DISTINCT split_part(s.auto, ' ', 1), c.country_id 
	FROM raw_data.sales s
	LEFT JOIN car_shop.country c USING(brand_origin)
	ORDER BY 1
RETURNING car_shop.brand.*;

---------

CREATE TABLE car_shop.colour (
	colour_id SERIAL PRIMARY KEY,
	colour VARCHAR(50) NOT NULL
);
INSERT INTO car_shop.colour (colour)
	SELECT DISTINCT split_part(s.auto, ', ', -1) colour
	FROM raw_data.sales s
	ORDER BY 1
RETURNING car_shop.colour.*;

---------
CREATE TABLE car_shop.cars (
	car_id SERIAL PRIMARY KEY,
	brand_id SERIAL REFERENCES car_shop.brand (brand_id) NOT NULL,
	model VARCHAR(100) NOT NULL,
	gasoline_consumption NUMERIC(3,1)
);

INSERT INTO car_shop.cars (brand_id, model, gasoline_consumption)
	SELECT DISTINCT
		b.brand_id, 
		TRIM(LTRIM(RTRIM(s.auto, ' ' || split_part(s.auto, ', ', -1)), split_part(s.auto, ' ', 1)), ', ') model,
		s.gasoline_consumption
	FROM raw_data.sales s
	LEFT JOIN car_shop.brand b ON b.brand_name = split_part(s.auto, ' ', 1)
	ORDER BY 1, 2
RETURNING car_shop.cars.*;

-------
CREATE TABLE car_shop.clients (
	client_id SERIAL PRIMARY KEY,
	person VARCHAR(100) NOT NULL,
	phone VARCHAR(50) NOT NULL UNIQUE
);

INSERT INTO car_shop.clients (person, phone)
	SELECT DISTINCT person, phone
		FROM raw_data.sales
	ORDER BY 1
RETURNING car_shop.clients.*;

---------
---------
CREATE TABLE car_shop.sales (
	sale_id SERIAL PRIMARY KEY,
	sale_date DATE NOT NULL DEFAULT CURRENT_DATE,
	price_with_discount NUMERIC(9,2) NOT NULL CONSTRAINT sales_price_positive CHECK(price_with_discount > 0),
	discount_percent INT NOT NULL CONSTRAINT sales_discount_positive CHECK(discount_percent >= 0) DEFAULT 0,
	client_id SERIAL REFERENCES car_shop.clients(client_id) NOT NULL,	
	car_id SERIAL REFERENCES car_shop.cars(car_id) NOT NULL,	
	colour_id SERIAL REFERENCES car_shop.colour(colour_id) NOT NULL
);

INSERT INTO car_shop.sales (sale_date, price_with_discount, discount_percent, client_id, car_id, colour_id)
		SELECT s.date, s.price, s.discount, cl.client_id, car.car_id, col.colour_id
		FROM car_shop.cars car
		JOIN car_shop.brand br USING(brand_id) 
		JOIN raw_data.sales s ON split_part(s.auto, ',', 1) = CONCAT(br.brand_name, ' ',car.model)
		JOIN car_shop.colour col ON col.colour = split_part(s.auto, ', ', -1)
		JOIN car_shop.clients cl USING(phone)
		ORDER BY s.date
RETURNING car_shop.sales.*;

----------------------------------
----------------------------------

--------ЭТАП 2--------------------
-- Задание 1
-- Напишите запрос, который выведет процент моделей машин, 
-- у которых нет параметра gasoline_consumption.

SELECT 
    ROUND((COUNT(s.*) * 100.0 / (SELECT COUNT(*) FROM car_shop.sales)), 1) nulls_percentage_gasoline_consumption
FROM  car_shop.sales s
JOIN car_shop.cars car USING(car_id)
WHERE car.gasoline_consumption IS NULL;
----------------------------------------

-- Задание 2
-- Напишите запрос, который покажет название бренда и среднюю цену его автомобилей 
-- в разбивке по всем годам с учётом скидки. Итоговый результат отсортируйте по названию 
-- бренда и году в восходящем порядке. Среднюю цену округлите до второго знака после запятой.

SELECT  b.brand_name, 
		EXTRACT(YEAR FROM s.sale_date)  "year", 
		ROUND(AVG(s.price_with_discount), 2) price_avg 
FROM car_shop.sales s
JOIN car_shop.cars c USING(car_id)
JOIN car_shop.brand b USING(brand_id)
GROUP BY 1, 2
ORDER BY 1, 2;
-----------------------------------------

-- Задание 3
-- Посчитайте среднюю цену всех автомобилей с разбивкой по месяцам в 2022 году с учётом скидки. 
-- Результат отсортируйте по месяцам в восходящем порядке. 
-- Среднюю цену округлите до второго знака после запятой.
SELECT  EXTRACT(MONTH FROM s.sale_date) "month", 
		EXTRACT(YEAR FROM s.sale_date) "year", 
		ROUND(AVG(s.price_with_discount), 2) price_avg 
FROM car_shop.sales s
WHERE EXTRACT(YEAR FROM s.sale_date) = 2022
GROUP BY 1, 2
ORDER BY 1;
-------------------------------------------

-- Задание 4
-- Используя функцию STRING_AGG, напишите запрос, 
-- который выведет список купленных машин у каждого пользователя через запятую. 
-- Пользователь может купить две одинаковые машины — это нормально. 
-- Название машины покажите полное, с названием бренда — например: Tesla Model 3. 
-- Отсортируйте по имени пользователя в восходящем порядке. 
-- Сортировка внутри самой строки с машинами не нужна.
SELECT cl.person, STRING_AGG(CONCAT(b.brand_name, ' ',c.model), ', ') cars 
FROM car_shop.clients cl
JOIN car_shop.sales s USING(client_id)
JOIN car_shop.cars c USING(car_id)
JOIN car_shop.brand b USING(brand_id)
GROUP BY cl.person
ORDER BY 1;
-------------------------------------------

-- Задание 5
-- Напишите запрос, который вернёт самую большую и самую маленькую цену продажи автомобиля 
-- с разбивкой по стране без учёта скидки. Цена в колонке price дана с учётом скидки.
SELECT country.brand_origin, 

	   MAX(CASE 
	   WHEN s.discount_percent > 0
	   THEN ROUND((s.price_with_discount * 100)/(100 - s.discount_percent), 2)
	   ELSE s.price_with_discount END) price_max,
	   
	   MIN(CASE 
	   WHEN s.discount_percent > 0
	   THEN ROUND((s.price_with_discount * 100)/(100 - s.discount_percent), 2)
	   ELSE s.price_with_discount END) price_min
	   
FROM car_shop.sales s
JOIN car_shop.cars c USING(car_id)
JOIN car_shop.brand b USING(brand_id)
JOIN car_shop.country USING(country_id)
GROUP BY 1
ORDER BY 1;
-------------------------------------------

-- Задание 6
-- Напишите запрос, который покажет количество всех пользователей из США. 
-- Это пользователи, у которых номер телефона начинается на +1.
SELECT COUNT(*) persons_from_usa_count FROM car_shop.clients
WHERE phone LIKE('+1%');