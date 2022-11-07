CREATE DATABASE IF NOT EXISTS house;

CREATE TABLE IF NOT EXISTS lifull_house_link (
	house_id VARCHAR(32) NOT NULL PRIMARY KEY,
	is_pr_item BOOLEAN NOT NULL DEFAULT FALSE,
	listing_house_name VARCHAR(255) NOT NULL,
	listing_house_price INT NOT NULL,
	sale_category VARCHAR(255) NOT NULL,
	city VARCHAR(255) NOT NULL,
	is_available BOOLEAN NOT NULL,
	first_available_date DATE NOT NULL,
	unavailable_date DATE
);


CREATE TABLE IF NOT EXISTS lifull_house_price_history (
	house_id VARCHAR(32) NOT NULL PRIMARY KEY,
	price INT NOT NULL,
	price_date DATE NOT NULL
);

