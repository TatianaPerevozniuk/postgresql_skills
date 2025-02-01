SELECT order_dt
FROM orders
WHERE order_id = 153;

SELECT order_id
FROM orders
WHERE order_dt > current_date::timestamp;

SELECT count(*)
FROM orders
WHERE user_id = '329551a1-215d-43e6-baee-322f2467272d';