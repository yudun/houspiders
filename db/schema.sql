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

CREATE TABLE IF NOT EXISTS lifull_rent_link (
	house_id VARCHAR(64) NOT NULL PRIMARY KEY,
	is_pr_item BOOLEAN NOT NULL DEFAULT FALSE,
	listing_house_name VARCHAR(255) NOT NULL,
	listing_house_rent INT NOT NULL,
	listing_house_manage_fee INT NOT NULL,
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

CREATE TABLE IF NOT EXISTS lifull_house_condition (
	house_id VARCHAR(32) NOT NULL PRIMARY KEY,
	house_condition VARCHAR(255) NOT NULL
);

CREATE TABLE IF NOT EXISTS lifull_stations_near_house (
	house_id VARCHAR(32) NOT NULL,
	line_name VARCHAR(255) NOT NULL,
	station_name VARCHAR(255) NOT NULL,
	walk_distance_in_minute INT NOT NULL,
	PRIMARY KEY(house_id, line_name, station_name)
);

CREATE TABLE IF NOT EXISTS lifull_house_info (
	house_id VARCHAR(32) NOT NULL PRIMARY KEY,
	name VARCHAR(255) NOT NULL,
	price INT NOT NULL,
	address VARCHAR(255) NOT NULL,
	moneykyoueki INT NOT NULL,
	moneyshuuzen INT NOT NULL,
	district VARCHAR(255) NOT NULL,
	build_date DATE NOT NULL,
	room VARCHAR(255),
	age INT,
	window_angle VARCHAR(255),
	house_area FLOAT,
	balcony_area FLOAT,
	has_balcony BOOLEAN NOT NULL,
	floor_plan TEXT,
	feature_comment TEXT,
	register_date DATE,
	has_elevator BOOLEAN NOT NULL,
	note TEXT,
	has_special_note BOOLEAN NOT NULL,
	unit_num INT,
	floor_num INT,
	num_total_floor INT,
	structure VARCHAR(255),
	land_usage VARCHAR(255),
	land_position VARCHAR(255),
	land_right VARCHAR(32),
	land_moneyshakuchi INT,
	land_term VARCHAR(255),
	land_landkokudoho VARCHAR(32),
	other_fee_details TEXT,
	total_other_fee INT NOT NULL,
	manage_details TEXT,
	latest_rent_status VARCHAR(32),
	trade_method VARCHAR(32)
);

CREATE TABLE IF NOT EXISTS lifull_crawler_stats (
	crawl_date DATE NOT NULL PRIMARY KEY,
	category VARCHAR(255) NOT NULL,
	city VARCHAR(255) NOT NULL,
	new_added_house_num INT NOT NULL DEFAULT 0,
	new_unavailable_become_available_house_num INT NOT NULL DEFAULT 0,
	updated_house_num INT NOT NULL DEFAULT 0,
	new_unavailable_house_num INT NOT NULL DEFAULT 0
);
