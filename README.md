# postgresql_skills
Этот репозиторий содержит примеры работы с PostgreSQL.
Проект демонстрирует навыки проектирования структуры баз данных и их наполнения данными. 
Cоздание и изменение базы данных и таблицы под конкретные задачи. 
Реализованы запросы различной сложности, включая использование оконных функций и рекурсии. 
Также реализован перенос бизнес-логики на сторону базы данных через создание процедур и функций. 
Проект включает оптимизацию запросов как на уровне базы данных, так и на уровне запросов, с использованием анализа плана запроса и других методов.

1. Основы SQL и баз данных.
В модуле project_1:
Этап 1. Создание и заполнение БД
Шаг 1. Создали БД с именем sprint_1.
Шаг 2. Создали схему raw_data и таблицу sales для загрузки сырых данных в этой схеме.
Шаг 3. Скачали исходный csv-файл  cars.csv
Шаг 4. Заполняем таблицу sales данными, используя команду COPY в менеджере БД (например, pgAdmin) или \copy в psql. Если возникнет ошибка о лишних данных в исходном файле, укажите явно параметр DELIMITER.
Шаг 5. Создаем схему car_shop, а в ней cоздаем нормализованные таблицы (до третьей нормальной формы)
Этап 2. Задания на выборку данных для аналитики.

Все задания/SQL-команды выполнены в итоговом файле sprint_1.sql


2. Продвинутый SQL для работы с данными.
В модуле project_2:
Развёртывание дампа для проекта
Скачать дамп sprint_2_dump.sql перед выполнением задания. Советую воспользоваться вариантом развёртывания через pgAdmin. Для этого достаточно: 
Создать новую БД.
Щёлкнуть правой кнопкой мыши на нужную базу и выбрать пункт “Restore…”.
В диалоговом окне “Restore…” выбрать скачанный файл.
Все остальные настройки оставить по умолчанию.

Все задания/SQL-команды выполнены в итоговом файле sprint_2.sql


3. Серверное программирование на SQL.
В модуле project_3:


Все задания/SQL-команды выполнены в итоговом файле sprint_3.sql


4. Оптимизация запросов.
В модуле project_4:


Все задания/SQL-команды выполнены в итоговых файлах sprint_4_part1.sql и sprint_4_part2.sql

