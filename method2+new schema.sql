-- DROP SCHEMA IF EXISTS reporting_schema CASCADE;
-- The CASCADE option ensures that all objects within the schema are also dropped.
-- CREATE SCHEMA reporting_schema;
DROP TABLE IF EXISTS reporting_schema.OLA_MHMD_AGG_DAILY;
CREATE TABLE IF NOT EXISTS reporting_schema.OLA_MHMD_AGG_DAILY
(
	day date PRIMARY KEY,
	total_films_rented INT,
	total_amount_paid NUMERIC,
	running_total_amount NUMERIC
);


-- first two
DROP TABLE IF EXISTS temp_TOTAL_FILMS_RENTED_PER_DAY;
CREATE TEMPORARY TABLE temp_TOTAL_FILMS_RENTED_PER_DAY AS 
	SELECT -- singers code
		CAST(payment_table.payment_date AS date) AS payment_day,
		COUNT(inventory_table.film_id) AS films_per_day,
		SUM(payment_table.amount) AS total_daily_payment
	FROM public.payment payment_table
	INNER JOIN public.rental rental_table
		ON rental_table.rental_id = payment_table.rental_id
	INNER JOIN public.inventory inventory_table
		ON inventory_table.inventory_id = rental_table.inventory_id
	GROUP BY CAST(payment_table.payment_date AS date)
	ORDER BY CAST(payment_table.payment_date AS date);

-- running total
DROP TABLE IF EXISTS temp_RUNNING_TOTAL;
CREATE TEMPORARY TABLE temp_RUNNING_TOTAL AS
	SELECT
		CAST(payment_date AS DATE) AS payment_day,
		SUM(amount) AS total_amount,
		SUM(SUM(amount)) OVER (ORDER BY CAST(payment_date AS DATE)) AS running_total
	FROM public.payment
	GROUP BY CAST(payment_date AS DATE)
	ORDER BY CAST(payment_date AS DATE);

-- inserting first 3 two columns
INSERT INTO reporting_schema.OLA_MHMD_AGG_DAILY(day,total_films_rented,total_amount_paid)
	SELECT
		payment_day,
		films_per_day,
		total_daily_payment
	FROM temp_TOTAL_FILMS_RENTED_PER_DAY;
	
-- the rest of the columns are set to null, so we use update and set to replace the null values with what we need
UPDATE reporting_schema.OLA_MHMD_AGG_DAILY
	SET
		running_total_amount = COALESCE(reporting_schema.OLA_MHMD_AGG_DAILY.running_total_amount,temp_RUNNING_TOTAL.running_total)
	FROM temp_RUNNING_TOTAL
	WHERE temp_RUNNING_TOTAL.payment_day = reporting_schema.OLA_MHMD_AGG_DAILY.day;

-- SELECT * FROM reporting_schema.OLA_MHMD_AGG_DAILY

-------------------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS reporting_schema.OLA_MHMD_AGG_MONTHLY;
CREATE TABLE IF NOT EXISTS reporting_schema.OLA_MHMD_AGG_MONTHLY
(
	month INT PRIMARY KEY,
	total_films_rented INT,
	total_amount_paid NUMERIC,
	running_total_amount NUMERIC
);

INSERT INTO reporting_schema.OLA_MHMD_AGG_MONTHLY(month,total_films_rented,total_amount_paid)
	SELECT
		EXTRACT(MONTH FROM payment_day),
		SUM(films_per_day),
		SUM(total_daily_payment)
	FROM temp_TOTAL_FILMS_RENTED_PER_DAY
	GROUP BY EXTRACT(MONTH FROM payment_day)
	ORDER BY EXTRACT(MONTH FROM payment_day);

WITH CTE_RUNNING_TOTAL_MONTHLY AS
(
	SELECT 
		EXTRACT(MONTH FROM payment_day) AS month,
		SUM(SUM(total_amount)) OVER (ORDER BY EXTRACT(MONTH FROM payment_day)) AS monthly_total_running
	FROM temp_RUNNING_TOTAL
	GROUP BY EXTRACT(MONTH FROM payment_day)
	ORDER BY EXTRACT(MONTH FROM payment_day)
)

UPDATE reporting_schema.OLA_MHMD_AGG_MONTHLY
	SET
		running_total_amount = COALESCE(reporting_schema.OLA_MHMD_AGG_MONTHLY.running_total_amount, monthly_total_running)
	FROM CTE_RUNNING_TOTAL_MONTHLY
	WHERE
		CTE_RUNNING_TOTAL_MONTHLY.month = reporting_schema.OLA_MHMD_AGG_MONTHLY.month;
	
SELECT * FROM reporting_schema.OLA_MHMD_AGG_MONTHLY