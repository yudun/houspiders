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
	house_id VARCHAR(32) NOT NULL,
	price INT NOT NULL,
	price_date DATE NOT NULL,
	PRIMARY KEY(house_id, price_date)
);

CREATE TABLE IF NOT EXISTS lifull_stations_near_house (
	house_id VARCHAR(32) NOT NULL,
	line_name VARCHAR(255) NOT NULL,
	station_name VARCHAR(255) NOT NULL,
	walk_distance_in_minute INT NOT NULL,
	PRIMARY KEY(house_id, line_name, walk_distance_in_minute)
);