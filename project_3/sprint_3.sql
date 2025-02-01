
-- Задание 1
-- В Dream Big ежемесячно оценивают производительность сотрудников. 
-- В результате бывает, кому-то повышают, а изредка понижают почасовую ставку. 
-- Напишем хранимую процедуру update_employees_rate, которая обновляет почасовую ставку сотрудников на определённый процент. 
-- При понижении ставка не может быть ниже минимальной — 500 рублей в час. 
-- Если по расчётам выходит меньше, устанавливают минимальную ставку.
-- На вход процедура принимает строку в формате json:

SELECT * FROM employees;

CREATE OR REPLACE PROCEDURE update_employees_rate (p_json_input json)
LANGUAGE plpgsql
AS $$
DECLARE
	change jsonb; --Переменная для итерации по элементам JSON
	employee_id uuid;
	rate_change integer;  --Процент изменения ставки
	current_rate integer;  --Текущая ставка сотрудника
	new_rate integer;  --Новая ставка после расчета
BEGIN
	--Проходим по каждому объекту в JSON-массиве
	FOR change IN SELECT * FROM jsonb_array_elements(p_json_input::jsonb)
	LOOP
		--Извлекаем id сотрудника и процент изменения ставки
		employee_id := (change->>'employee_id')::uuid;
		rate_change := (change->>'rate_change')::integer;

		--Проверим существует ли сотрудник и запишем текущую ставку в переменную 
		SELECT rate
		INTO current_rate
		FROM employees
		WHERE id = employee_id;

		IF NOT FOUND THEN
			RAISE NOTICE 'Employee with ID % not found, skipping.', employee_id;
			CONTINUE;
		END IF;

		--Вычисляем новую ставку
		new_rate := current_rate + (current_rate * rate_change / 100);

		--Если новая ставка меньше минимальной, устанавливаем минимальную ставку
		IF new_rate < 500 THEN
			new_rate := 500;
		END IF;

		--Обновляем ставку сотрудника
		UPDATE employees
		SET rate = new_rate
		WHERE id = employee_id;

		RAISE NOTICE 'Updated employee %: rate changed from % to %.', employee_id, current_rate, new_rate;
	END LOOP;
END;
$$;

--Вызываем процедуру
CALL update_employees_rate('[
    {"employee_id": "dd0ba8dd-6c75-437c-9c68-824971ccc078", "rate_change": 10}, 
    {"employee_id": "5a6aed8f-8f53-4931-82f4-66673633f2a8", "rate_change": -5}
]'::json);

--Проверяем
SELECT * FROM employees
WHERE id IN ('dd0ba8dd-6c75-437c-9c68-824971ccc078', '5a6aed8f-8f53-4931-82f4-66673633f2a8');



-- Задание 2
-- С ростом доходов компании и учётом ежегодной инфляции Dream Big индексирует зарплату всем сотрудникам.
-- Напишите хранимую процедуру indexing_salary, которая повышает зарплаты всех сотрудников на определённый процент. 
-- Процедура принимает один целочисленный параметр — процент индексации p. 
-- Сотрудникам, которые получают зарплату по ставке ниже средней относительно всех сотрудников до индексации, начисляют дополнительные 2% (p + 2). 
-- Ставка остальных сотрудников увеличивается на p%.
-- Зарплата хранится в БД в типе данных integer, поэтому если в результате повышения зарплаты образуется дробное число, его нужно округлить до целого.

CREATE OR REPLACE PROCEDURE indexing_salary(p_index integer)
LANGUAGE plpgsql
AS $$
DECLARE
	avg_rate integer;
BEGIN
	-- Вычисление средней зарплаты
	SELECT ROUND(AVG(rate))
	INTO avg_rate
	FROM employees;

	-- Обновление зарплат всех сотрудников
	UPDATE employees
    SET rate = ROUND(
        rate * (1 + CASE 
                       WHEN rate < avg_rate THEN (p_index + 2)::numeric / 100
                       ELSE p_index::numeric / 100
                   END)
    );
	
END;
$$;


CALL indexing_salary(5);  -- для индексации зарплаты на 5%

SELECT * FROM employees;


--Задание 3
--Завершая проект, нужно сделать два действия в системе учёта:
--Изменить значение поля is_active в записи проекта на false — чтобы рабочее время по этому проекту больше не учитывалось.
--Посчитать бонус, если он есть — то есть распределить неизрасходованное время между всеми членами команды проекта. 
--Неизрасходованное время — это разница между временем, которое выделили на проект (estimated_time), и фактически потраченным. 
--Если поле estimated_time не задано, бонусные часы не распределятся. Если отработанных часов нет — расчитывать бонус не нужно.
--Разберёмся с бонусом. 
--Если в момент закрытия проекта estimated_time:
-- 1. не NULL,
-- 2. больше суммы всех отработанных над проектом часов,
-- всем членам команды проекта начисляют бонусные часы.
--Размер бонуса считают так: 75% от сэкономленных часов делят на количество участников проекта, но не более 16 бонусных часов на сотрудника. 
--Дробные значения округляют в меньшую сторону (например, 3.7 часа округляют до 3). Рабочие часы заносят в логи с текущей датой. 
--Например, если на проект запланировали 100 часов, а сделали его за 30 — 3/4 от сэкономленных 70 часов распределят бонусом между участниками проекта.
--Создадим пользовательскую процедуру завершения проекта close_project. 
--Если проект уже закрыт, процедура должна вернуть ошибку без начисления бонусных часов.

CREATE OR REPLACE PROCEDURE close_project(p_project_id uuid)
LANGUAGE plpgsql
AS $$
DECLARE
	estimated_time integer;
	is_active boolean;
	total_worked_hours integer;
	participant_count integer;
	unspent_hours integer;
	bonus_hours_per_employee integer;
	
BEGIN
	-- Проверяем активность проекта
	 SELECT p.estimated_time, p.is_active
	 INTO estimated_time, is_active
	 FROM projects p
	 WHERE p.id = p_project_id;

	 IF NOT FOUND OR NOT is_active THEN
	 	RAISE EXCEPTION 'Проект уже закрыт или не существует';
	 END IF;

	--Считаем отработанные часы по проекту и количество уникальных участников
	 SELECT COALESCE(SUM(work_hours), 0), COUNT(DISTINCT employee_id)
	 INTO total_worked_hours, participant_count
	 FROM logs
	 WHERE project_id = p_project_id;

    -- Если нет участников или estimated_time не задан или нет сэкономленных часов, закрываем проект
	 IF participant_count = 0 OR estimated_time IS NULL OR estimated_time <= total_worked_hours THEN
	 	UPDATE projects 
		SET is_active = false 
		WHERE id = p_project_id;
		RETURN;
	 END IF;

	 -- Считаем сэкономленные часы и колличество участников
	 unspent_hours := estimated_time - total_worked_hours;

	 -- Считаем бонусные часы на сотрудника, ограничиваем максимум 16 часов на сотрудника
	 bonus_hours_per_employee := LEAST(FLOOR((unspent_hours * 0.75) / participant_count), 16);

	 -- Если бонусные часы больше 0, вносим их в логи
     IF bonus_hours_per_employee > 0 THEN
	 	INSERT INTO logs (employee_id, project_id, work_date, work_hours, required_review, is_paid)
	 	SELECT DISTINCT employee_id, p_project_id, CURRENT_DATE, bonus_hours_per_employee, false, false
	 	FROM logs
	 	WHERE project_id = p_project_id;
	 END IF;
	 --Закрываем проект
	 UPDATE projects 
	 SET is_active = false 
	 WHERE id = p_project_id;
		 
END;
$$;

CALL close_project('4abb5b99-3889-4c20-a575-e65886f266f9'); 

SELECT * FROM projects
WHERE id = '4abb5b99-3889-4c20-a575-e65886f266f9';

SELECT * FROM logs
WHERE project_id = '4abb5b99-3889-4c20-a575-e65886f266f9';



-- Задание 4
/* 
Напишите процедуру log_work для внесения отработанных сотрудниками часов. 
Процедура добавляет новые записи о работе сотрудников над проектами.
Процедура принимает id сотрудника, id проекта, дату и отработанные часы и вносит данные в таблицу logs. 
Если проект завершён, добавить логи нельзя — процедура должна вернуть ошибку Project closed. 
Количество залогированных часов может быть в этом диапазоне: от 1 до 24 включительно — нельзя внести менее 1 часа или больше 24. 
Если количество часов выходит за эти пределы, необходимо вывести предупреждение о недопустимых данных и остановить выполнение процедуры.
Запись помечается флагом required_review, если:
- залогированно более 16 часов за один день — Dream Big заботится о здоровье сотрудников;
- запись внесена будущим числом;
- запись внесена более ранним числом, чем на неделю назад от текущего дня — например, если сегодня 10.04.2023, все записи старше 3.04.2023 получат флажок.
*/

CREATE OR REPLACE PROCEDURE log_work(p_employee_id uuid, p_project_id uuid,
									p_work_date date, p_work_hours integer)
LANGUAGE plpgsql
AS $$
DECLARE
	is_project_active boolean;
    requires_review boolean;
BEGIN
	-- Проверка: существует ли проект и активен ли он
	SELECT is_active 
	INTO is_project_active
	FROM projects
	WHERE id = p_project_id;
	
	IF NOT FOUND OR NOT is_project_active THEN
		RAISE EXCEPTION 'Project closed or does not exist';
	END IF;

	-- Проверка: количество часов от 1 до 24
	IF p_work_hours < 1 OR p_work_hours > 24 THEN
		RAISE EXCEPTION 'Invalid work hours: must be between 1 and 24';
	END IF;

	-- Проверка: требуется ли ревью
	requires_review := 
        p_work_hours > 16 OR               -- Более 16 часов в день
        p_work_date > CURRENT_DATE OR      -- Будущая дата
        p_work_date < CURRENT_DATE - 7;    -- Дата старше недели

	-- Добавление записи в таблицу logs
	INSERT INTO logs (employee_id, project_id, work_date, work_hours, required_review)
    VALUES (p_employee_id, p_project_id, p_work_date, p_work_hours, requires_review);
	
END;
$$;

CALL log_work(
    '6db4f4a3-239b-4085-a3f9-d1736040b38c', -- employee uuid
    '35647af3-2aac-45a0-8d76-94bc250598c2', -- project uuid
    '2024-12-08',                           -- work date
    14                                      -- worked hours
); 

SELECT * FROM logs
WHERE project_id = '35647af3-2aac-45a0-8d76-94bc250598c2';



-- Задание 5
/* 
Чтобы бухгалтерия корректно начисляла зарплату, нужно хранить историю изменения почасовой ставки сотрудников. 
Создайте отдельную таблицу employee_rate_history с такими столбцами:
id — id записи,
employee_id — id сотрудника,
rate — почасовая ставка сотрудника,
from_date — дата назначения новой ставки.
Внесите в таблицу текущие данные всех сотрудников. 
В качестве from_date используйте дату основания компании: '2020-12-26'.
Напишите триггерную функцию save_employee_rate_history и триггер change_employee_rate. 
При добавлении сотрудника в таблицу employees и изменении ставки сотрудника 
триггер автоматически вносит запись в таблицу employee_rate_history из трёх полей: 
id сотрудника, его ставки и текущей даты.
*/

--Создадим таблицу employee_rate_history
CREATE TABLE IF NOT EXISTS employee_rate_history (
    id uuid NOT NULL DEFAULT gen_random_uuid(),
    employee_id uuid NOT NULL,
    rate integer NOT NULL,
    from_date date NOT NULL,
    CONSTRAINT employee_rate_history_pkey PRIMARY KEY (id),
    CONSTRAINT employee_rate_history_employee_fk FOREIGN KEY (employee_id)
        REFERENCES employees (id) MATCH SIMPLE
        ON UPDATE CASCADE
        ON DELETE CASCADE
);

--Внесение текущих данных сотрудников в таблицу employee_rate_history
INSERT INTO employee_rate_history (employee_id, rate, from_date)
SELECT id, rate, '2020-12-26'::date
FROM employees;

--Триггерная функция save_employee_rate_history
CREATE OR REPLACE FUNCTION save_employee_rate_history()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
	-- Добавление записи в таблицу employee_rate_history
	INSERT INTO employee_rate_history (employee_id, rate, from_date)
	VALUES (NEW.id, NEW.rate, CURRENT_DATE);

	RETURN NEW;
END;
$$;

--Создание триггера change_employee_rate
CREATE OR REPLACE TRIGGER change_employee_rate
AFTER INSERT OR UPDATE OF rate ON employees
FOR EACH ROW
EXECUTE FUNCTION save_employee_rate_history();

--Проверка
INSERT INTO employees (name, email, rate)
VALUES ('Kate Doe', 'kate.doe@google.com', 35);

UPDATE employees
SET rate = 45
WHERE email = 'kate.doe@google.com'; 

SELECT * FROM employee_rate_history
WHERE from_date = CURRENT_DATE;



-- Задание 6
/*
После завершения каждого проекта Dream Big проводит корпоративную вечеринку, 
чтобы отпраздновать очередной успех и поощрить сотрудников. 
Тех, кто посвятил проекту больше всего часов, награждают 
премией «Айтиголик» — они получают почётные грамоты и ценные подарки от заказчика.

Чтобы вычислить айтиголиков проекта, напишите функцию best_project_workers.

Функция принимает id проекта и возвращает таблицу с именами трёх сотрудников, 
которые залогировали максимальное количество часов в этом проекте. 
Результирующая таблица состоит из двух полей: имени сотрудника и количества часов, отработанных на проекте.
*/

CREATE OR REPLACE FUNCTION best_project_workers(p_project_id uuid)
RETURNS TABLE (
	employee_name TEXT,
    total_work_hours INTEGER
)
LANGUAGE plpgsql
AS $$
BEGIN

	RETURN QUERY
	SELECT e.name AS employee_name,
			SUM(l.work_hours)::INTEGER AS total_work_hours
	FROM logs l
	JOIN employees e ON l.employee_id = e.id
	WHERE l.project_id = p_project_id
	GROUP BY e.id, e.name
	ORDER BY total_work_hours DESC
	LIMIT 3;
	
END;
$$;

--Проверка
SELECT employee_name, total_work_hours 
FROM best_project_workers('2dfffa75-7cd9-4426-922c-95046f3d06a0'); 



-- Задание 6*
/*
Доработайте функцию best_project_workers.
Если обнаружится несколько сотрудников с одинаковым количеством залогированных часов, 
первым становится тот, кто залогировал больше дней. 
Если и этот параметр совпадёт, сотрудники в списке выводятся в рандомном порядке. 
Максимальное количество человек в списке — три.
*/

DROP FUNCTION IF EXISTS best_project_workers(uuid);

CREATE OR REPLACE FUNCTION best_project_workers(p_project_id uuid)
RETURNS TABLE (
    employee_name TEXT,
    total_work_hours INTEGER,
    total_work_days INTEGER
)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    SELECT 
        e.name AS employee_name,
        SUM(l.work_hours)::INTEGER AS total_work_hours,
        COUNT(DISTINCT l.work_date)::INTEGER AS total_work_days
    FROM logs l
    JOIN employees e ON l.employee_id = e.id
    WHERE l.project_id = p_project_id
    GROUP BY e.id, e.name
    ORDER BY 
        total_work_hours DESC,      -- Сортировка по общему количеству часов
        total_work_days DESC,       -- Затем по количеству уникальных дней
        random()                    -- Для случайного порядка при совпадении
    LIMIT 3;                        
END;
$$;

--Проверка
SELECT employee_name, total_work_hours, total_work_days
FROM best_project_workers('2dfffa75-7cd9-4426-922c-95046f3d06a0'); 



-- Задание 7
/*
К вам заглянул утомлённый главный бухгалтер Марк Захарович с лёгкой синевой под глазами 
и попросил как-то автоматизировать расчёт зарплаты, пока бухгалтерия не испустила дух.
Напишите для бухгалтерии функцию calculate_month_salary для расчёта зарплаты за месяц.
Функция принимает в качестве параметров даты начала и конца месяца и возвращает результат 
в виде таблицы с четырьмя полями: id (сотрудника), employee (имя сотрудника), worked_hours и salary.
Процедура суммирует все залогированные часы за определённый месяц и умножает на актуальную почасовую ставку сотрудника. 
Исключения — записи с флажками required_review и is_paid.
Если суммарно по всем проектам сотрудник отработал более 160 часов в месяц, 
все часы свыше 160 оплатят с коэффициентом 1.25.
*/


CREATE OR REPLACE FUNCTION calculate_month_salary(start_date date, end_date date)
RETURNS TABLE (
    id uuid,
    employee TEXT,
    worked_hours INTEGER,
    salary NUMERIC
)
LANGUAGE plpgsql
AS $$
DECLARE
    hourly_rate INTEGER;
    base_hours INTEGER := 160;
    extra_pay_multiplier NUMERIC := 1.25;
BEGIN
    RETURN QUERY
    SELECT 
        e.id,
        e.name AS employee,
        SUM(l.work_hours)::INTEGER AS worked_hours,
        CASE
            WHEN SUM(l.work_hours) > base_hours THEN
                (base_hours * e.rate) + ((SUM(l.work_hours) - base_hours) * e.rate * extra_pay_multiplier)
            ELSE
                SUM(l.work_hours) * e.rate
        END AS salary
    FROM logs l
    JOIN employees e ON l.employee_id = e.id
    WHERE 
        l.work_date BETWEEN start_date AND end_date
        AND l.required_review = false
        AND l.is_paid = false
    GROUP BY e.id, e.name, e.rate
    ORDER BY salary DESC;
END;
$$;

SELECT * FROM calculate_month_salary(
    '2023-10-01',  -- start of month
    '2023-10-31'   -- end of month
); 



-- Задание 7*
/*
Доработайте функцию calculate_month_salary.
Если у сотрудника есть флажки required_review, при выполнении функции 
появится предупреждение: Warning! Employee % hours must be reviewed!.
На часы, не требующие проверки менеджера, это не повлияет 
— зарплату за них начислят в обычном режиме.
*/

DROP FUNCTION IF EXISTS calculate_month_salary(date, date);

CREATE OR REPLACE FUNCTION calculate_month_salary(start_date date, end_date date)
RETURNS TABLE (
    id uuid,
    employee TEXT,
    worked_hours INTEGER,
    salary NUMERIC,
    warning TEXT
)
LANGUAGE plpgsql
AS $$
DECLARE
    base_hours INTEGER := 160;
    extra_pay_multiplier NUMERIC := 1.25;
BEGIN
    RETURN QUERY
    SELECT 
        e.id,
        e.name AS employee,
		-- Считаем только часы без флага required_review
		SUM(CASE WHEN l.required_review = false THEN l.work_hours 
				ELSE 0 END)::INTEGER AS worked_hours,
		-- Расчёт зарплаты
        CASE
            WHEN SUM(CASE WHEN l.required_review = false THEN l.work_hours ELSE 0 END) > base_hours THEN
                (base_hours * e.rate) + 
				((SUM(CASE WHEN l.required_review = false THEN l.work_hours ELSE 0 END) - base_hours) * e.rate * extra_pay_multiplier)
            ELSE
                SUM(CASE WHEN l.required_review = false THEN l.work_hours ELSE 0 END) * e.rate
        END AS salary,
		-- Предупреждение, если есть часы с флагом required_review
        CASE
            WHEN EXISTS (
                SELECT 1 FROM logs l2 
                WHERE l2.employee_id = e.id 
                AND l2.work_date BETWEEN start_date AND end_date 
                AND l2.required_review = true
            )
            THEN 'Warning! Employee ' || e.name || ' hours must be reviewed!'
            ELSE NULL
        END AS warning
    FROM logs l
    JOIN employees e ON l.employee_id = e.id
    WHERE 
        l.work_date BETWEEN start_date AND end_date
        AND l.is_paid = false
    GROUP BY e.id, e.name, e.rate
    ORDER BY salary DESC;
END;
$$;

--Проверка
SELECT * FROM calculate_month_salary('2023-10-01', '2023-10-31');
