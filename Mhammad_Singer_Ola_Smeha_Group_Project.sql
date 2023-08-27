-- DROP SCHEMA IF EXISTS reporting_schema CASCADE;
-- The CASCADE option ensures that all objects within the schema are also dropped.
CREATE SCHEMA reporting_schema;
DROP TABLE IF EXISTS reporting_schema.OLA_MHMD_AGG_DAILY;
CREATE TABLE IF NOT EXISTS reporting_schema.OLA_MHMD_AGG_DAILY
(
	date date PRIMARY KEY,
	total_films_rented INT,
	total_amount_paid NUMERIC,
	running_total_amount NUMERIC,
	total_top_category_sports_movies NUMERIC,
	total_least_category_music_movies NUMERIC,
	total_customers INT,
	total_active_customers INT,
	active_customers_perc NUMERIC,
	total_distinct_cust_cities INT
);


-- TOTAL FILMS RENTED & TOTAL AMOUNT PAID
DROP TABLE IF EXISTS temp_TOTAL_FILMS_RENTED_PER_DAY;
CREATE TEMPORARY TABLE temp_TOTAL_FILMS_RENTED_PER_DAY AS 
	SELECT 
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

-- RUNNING TOTAL
DROP TABLE IF EXISTS temp_RUNNING_TOTAL;
CREATE TEMPORARY TABLE temp_RUNNING_TOTAL AS
	SELECT
		CAST(payment_date AS DATE) AS payment_day,
		SUM(amount) AS total_amount,
		SUM(SUM(amount)) OVER (ORDER BY CAST(payment_date AS DATE)) AS running_total
	FROM public.payment
	GROUP BY CAST(payment_date AS DATE)
	ORDER BY CAST(payment_date AS DATE);
	
-- TOP AND LEAST CATEGORIES
DROP TABLE IF EXISTS temp_CATEGORIES_PER_DAY;
CREATE TEMPORARY TABLE temp_CATEGORIES_PER_DAY AS
SELECT 
	CAST(payment.payment_date AS DATE) AS payment_day,
	COUNT(film_category.category_id) AS total_films_count,
	COUNT
	(
		CASE 
			WHEN film_category.category_id = 15
			THEN 1				
		END
	) AS top_category_sports,
	COUNT
	(
		CASE 
			WHEN film_category.category_id = 12
			THEN 1
		END
	) AS least_category_music
FROM payment
INNER JOIN public.rental AS rentals
	ON payment.rental_id= rentals.rental_id
INNER JOIN inventory AS inventory
	ON rentals.inventory_id= inventory.inventory_id
INNER JOIN film_category AS film_category
	ON film_category.film_id=inventory.film_id
GROUP BY CAST(payment.payment_date AS DATE)
ORDER BY CAST(payment.payment_date AS DATE);

-- TOTAL CUSTOMERS, TOTAL ACIVE CUSTOMERS, ACTIVE CUSTOMERS PERCENTAGE
DROP TABLE IF EXISTS temp_CUSTOMERS_DETAILS;
CREATE TEMPORARY TABLE temp_CUSTOMERS_DETAILS AS
WITH CTE_ALL_CUSTOMERS AS
(
SELECT
	CAST(payment.payment_date AS DATE) AS payment_date,
	COUNT(customer.customer_id) AS total_customers,
	COUNT(CASE WHEN customer.active = 1 THEN customer.customer_id END) AS total_active_customers
FROM public.customer AS customer
INNER JOIN public.payment AS payment
	ON customer.customer_id = payment.customer_id
GROUP BY CAST(payment.payment_date AS DATE)
ORDER BY CAST(payment.payment_date AS DATE)
)
SELECT
	payment_date,
	total_customers,
	total_active_customers,
	ROUND
	(
		CAST(total_active_customers AS NUMERIC) /
		CAST(NULLIF(total_customers,0) AS NUMERIC)*100
	,2) AS active_customers_perc
FROM CTE_ALL_CUSTOMERS;

-- TOTAL DISTINCT CUSTOMER CITIES
DROP TABLE IF EXISTS temp_TOTAL_DISTINCT_CUSTOMER_CITIES;
CREATE TEMPORARY TABLE temp_TOTAL_DISTINCT_CUSTOMER_CITIES AS
SELECT
	CAST(payment.payment_date AS DATE) AS payment_day,
	COUNT(DISTINCT city.city_id) AS total_cities
FROM public.city
INNER JOIN public.address
	ON address.city_id = city.city_id
INNER JOIN public.customer
	ON customer.address_id = address.address_id
INNER JOIN public.payment
	ON payment.customer_id = customer.customer_id
GROUP BY CAST(payment.payment_date AS DATE)
ORDER BY CAST(payment.payment_date AS DATE);


-- INSERTING DATA INTO DAILY TABLE
INSERT INTO reporting_schema.OLA_MHMD_AGG_DAILY
	(
		date,
		total_films_rented,
		total_amount_paid,
		running_total_amount,
		total_top_category_sports_movies,
		total_least_category_music_movies,
		total_customers,
		total_active_customers,
		active_customers_perc,
		total_distinct_cust_cities
	)
	SELECT
		temp_TOTAL_FILMS_RENTED_PER_DAY.payment_day,
		temp_TOTAL_FILMS_RENTED_PER_DAY.films_per_day,
		temp_TOTAL_FILMS_RENTED_PER_DAY.total_daily_payment,
		temp_RUNNING_TOTAL.running_total,
		temp_CATEGORIES_PER_DAY.top_category_sports,
		temp_CATEGORIES_PER_DAY.least_category_music,
		temp_CUSTOMERS_DETAILS.total_customers,
		temp_CUSTOMERS_DETAILS.total_active_customers,
		temp_CUSTOMERS_DETAILS.active_customers_perc,
		temp_TOTAL_DISTINCT_CUSTOMER_CITIES.total_cities
	FROM temp_TOTAL_FILMS_RENTED_PER_DAY
	LEFT JOIN temp_RUNNING_TOTAL
		ON temp_TOTAL_FILMS_RENTED_PER_DAY.payment_day = temp_RUNNING_TOTAL.payment_day
	LEFT JOIN temp_CATEGORIES_PER_DAY
		ON temp_TOTAL_FILMS_RENTED_PER_DAY.payment_day = temp_CATEGORIES_PER_DAY.payment_day
	LEFT JOIN temp_CUSTOMERS_DETAILS
		ON temp_TOTAL_FILMS_RENTED_PER_DAY.payment_day = temp_CUSTOMERS_DETAILS.payment_date
	LEFT JOIN temp_TOTAL_DISTINCT_CUSTOMER_CITIES
		ON temp_TOTAL_FILMS_RENTED_PER_DAY.payment_day = temp_TOTAL_DISTINCT_CUSTOMER_CITIES.payment_day
	;


-- MONTHLY AGGREGATE TABLE
DROP TABLE IF EXISTS reporting_schema.OLA_MHMD_AGG_MONTHLY;
CREATE TABLE reporting_schema.OLA_MHMD_AGG_MONTHLY AS 
	WITH CTE_CUST_CITIES_MONTHLY AS
	(
		SELECT
			EXTRACT(MONTH FROM payment.payment_date) AS payment_month,
			EXTRACT(YEAR FROM payment.payment_date) AS payment_year,
			COUNT(DISTINCT city.city_id) AS total_cities
		FROM public.city city
		INNER JOIN public.address address
			ON address.city_id = city.city_id
		INNER JOIN public.customer customer
			ON customer.address_id = address.address_id
		INNER JOIN public.payment payment
			ON payment.customer_id = customer.customer_id
		GROUP BY
			EXTRACT(MONTH FROM payment.payment_date),
			EXTRACT(YEAR FROM payment.payment_date)
		ORDER BY
			EXTRACT(MONTH FROM payment.payment_date),
			EXTRACT(YEAR FROM payment.payment_date)
	)
	SELECT 
		EXTRACT(MONTH FROM daily_agg.date) AS month,
		EXTRACT(YEAR FROM daily_agg.date) AS year,
		SUM(daily_agg.total_films_rented) AS total_films_rented,
		SUM(daily_agg.total_amount_paid) AS total_amount_paid,
		SUM(SUM(daily_agg.total_amount_paid)) OVER (ORDER BY EXTRACT(MONTH FROM daily_agg.date)) AS running_total,
		SUM(daily_agg.total_top_category_sports_movies) AS total_top_category_sports_movies,
		SUM(daily_agg.total_least_category_music_movies) AS total_least_category_music_movies,
		SUM(daily_agg.total_customers) AS total_customers,
		SUM(daily_agg.total_active_customers) AS total_active_customers,
		ROUND
		(
		CAST(SUM(daily_agg.total_active_customers) AS NUMERIC) /
		CAST(NULLIF(SUM(daily_agg.total_customers),0) AS NUMERIC)*100
		,2) AS active_customers_perc,
		CTE_CUST_CITIES_MONTHLY.total_cities AS total_distinct_cust_cities
	FROM reporting_schema.OLA_MHMD_AGG_DAILY AS daily_agg
	INNER JOIN CTE_CUST_CITIES_MONTHLY
		ON CTE_CUST_CITIES_MONTHLY.payment_month = EXTRACT(MONTH FROM daily_agg.date)
	GROUP BY
		EXTRACT(MONTH FROM daily_agg.date),
		EXTRACT(YEAR FROM daily_agg.date),
		CTE_CUST_CITIES_MONTHLY.total_cities
	ORDER BY
		EXTRACT(MONTH FROM daily_agg.date),
		EXTRACT(YEAR FROM daily_agg.date)
	;

-- YEARLY AGGREGATE TABLE
DROP TABLE IF EXISTS reporting_schema.OLA_MHMD_AGG_YEARLY;
CREATE TABLE reporting_schema.OLA_MHMD_AGG_YEARLY AS 
	WITH CTE_CUST_CITIES_YEARLY AS
	(
		SELECT
			EXTRACT(YEAR FROM payment.payment_date) AS payment_year,
			COUNT(DISTINCT city.city_id) AS total_cities
		FROM public.city city
		INNER JOIN public.address address
			ON address.city_id = city.city_id
		INNER JOIN public.customer customer
			ON customer.address_id = address.address_id
		INNER JOIN public.payment payment
			ON payment.customer_id = customer.customer_id
		GROUP BY
			EXTRACT(YEAR FROM payment.payment_date)
		ORDER BY
			EXTRACT(YEAR FROM payment.payment_date)
	)
	SELECT 
		EXTRACT(YEAR FROM monthly_agg.date) AS year,
		SUM(monthly_agg.total_films_rented) AS total_films_rented,
		SUM(monthly_agg.total_amount_paid) AS total_amount_paid,
		SUM(SUM(monthly_agg.total_amount_paid)) OVER (ORDER BY EXTRACT(YEAR FROM monthly_agg.date)) AS running_total,
		SUM(monthly_agg.total_top_category_sports_movies) AS total_top_category_sports_movies,
		SUM(monthly_agg.total_least_category_music_movies) AS total_least_category_music_movies,
		SUM(monthly_agg.total_customers) AS total_customers,
		SUM(monthly_agg.total_active_customers) AS total_active_customers,
		ROUND
		(
		CAST(SUM(monthly_agg.total_active_customers) AS NUMERIC) /
		CAST(NULLIF(SUM(monthly_agg.total_customers),0) AS NUMERIC)*100
		,2) AS active_customers_perc,
		CTE_CUST_CITIES_YEARLY.total_cities AS total_distinct_cust_cities
		
	FROM reporting_schema.OLA_MHMD_AGG_DAILY AS monthly_agg
	INNER JOIN CTE_CUST_CITIES_YEARLY
		ON CTE_CUST_CITIES_YEARLY.payment_year = EXTRACT(YEAR FROM monthly_agg.date)
	GROUP BY
		EXTRACT(YEAR FROM monthly_agg.date),
		CTE_CUST_CITIES_YEARLY.total_cities
	ORDER BY EXTRACT(YEAR FROM monthly_agg.date)
	;
	
SELECT * FROM reporting_schema.OLA_MHMD_AGG_DAILY;
SELECT * FROM reporting_schema.OLA_MHMD_AGG_MONTHLY;
SELECT * FROM reporting_schema.OLA_MHMD_AGG_YEARLY;


