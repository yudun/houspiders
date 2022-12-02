CREATE DATABASE IF NOT EXISTS house;

CREATE TABLE IF NOT EXISTS lifull_house_link (
	house_id VARCHAR(64) NOT NULL,
	is_pr_item BOOLEAN NOT NULL DEFAULT FALSE,
	listing_house_name VARCHAR(255) NOT NULL,
	listing_house_price FLOAT NOT NULL,
	sale_category VARCHAR(255) NOT NULL,
	city VARCHAR(255) NOT NULL,
	is_available BOOLEAN NOT NULL,
	first_available_date DATE NOT NULL,
	unavailable_date DATE,
	PRIMARY KEY(house_id, sale_category)
);

CREATE TABLE IF NOT EXISTS lifull_rent_link (
	house_id VARCHAR(64) NOT NULL PRIMARY KEY,
	is_pr_item BOOLEAN NOT NULL DEFAULT FALSE,
	listing_house_name VARCHAR(255) NOT NULL,
	listing_house_rent FLOAT NOT NULL,
	listing_house_manage_fee FLOAT NOT NULL,
	city VARCHAR(255) NOT NULL,
	is_available BOOLEAN NOT NULL,
	first_available_date DATE NOT NULL,
	unavailable_date DATE
);

CREATE TABLE IF NOT EXISTS lifull_house_price_history (
	house_id VARCHAR(64) NOT NULL,
	price FLOAT NOT NULL,
	price_date DATE NOT NULL,
	PRIMARY KEY(house_id, price_date)
);

CREATE TABLE IF NOT EXISTS lifull_rent_price_history (
	house_id VARCHAR(64) NOT NULL,
	rent FLOAT NOT NULL,
	manage_fee FLOAT NOT NULL,
	price_date DATE NOT NULL,
	PRIMARY KEY(house_id, price_date)
);

CREATE TABLE IF NOT EXISTS lifull_house_condition (
	house_id VARCHAR(64) NOT NULL,
	house_condition VARCHAR(255) NOT NULL,
	category VARCHAR(255) NOT NULL,
	PRIMARY KEY(house_id, house_condition)
);

CREATE TABLE IF NOT EXISTS lifull_stations_near_house (
	house_id VARCHAR(64) NOT NULL,
	line_name VARCHAR(255) NOT NULL,
	station_name VARCHAR(255) NOT NULL,
	walk_distance_in_minute INT NOT NULL,
	category VARCHAR(255) NOT NULL,
	PRIMARY KEY(house_id, line_name, station_name)
);

CREATE TABLE IF NOT EXISTS lifull_house_info (
	house_id VARCHAR(64) NOT NULL PRIMARY KEY,
	name VARCHAR(255) NOT NULL,
	price FLOAT NOT NULL,
	address VARCHAR(255) NOT NULL,
	moneykyoueki FLOAT NOT NULL,
	moneyshuuzen FLOAT NOT NULL,
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
	land_moneyshakuchi FLOAT,
	land_term VARCHAR(255),
	land_landkokudoho VARCHAR(32),
	other_fee_details TEXT,
	total_other_fee FLOAT NOT NULL,
	manage_details TEXT,
	latest_rent_status VARCHAR(32),
	trade_method VARCHAR(32)
);

CREATE TABLE IF NOT EXISTS lifull_rent_info (
	house_id VARCHAR(64) NOT NULL PRIMARY KEY,
	name VARCHAR(255) NOT NULL,
	room VARCHAR(255),
	rent FLOAT NOT NULL,
	manage_fee FLOAT NOT NULL,
	deposit_money_in_month FLOAT NOT NULL,
	gift_money_in_month FLOAT NOT NULL,
	guarantee_money_in_month FLOAT NOT NULL,
	shokyaku_money_in_month FLOAT NOT NULL,
	address VARCHAR(255) NOT NULL,
	district VARCHAR(255) NOT NULL,
	build_date DATE NOT NULL,
	age INT,
	window_angle VARCHAR(255),
	house_area FLOAT,
	balcony_area FLOAT,
	has_balcony BOOLEAN NOT NULL,
	floor_plan TEXT,
	other_fee_details TEXT,
	total_other_fee FLOAT NOT NULL,
	structure VARCHAR(255),
	parking_lot VARCHAR(255),
	unit_num INT,
	floor_num INT,
	num_total_floor INT,
	rent_term VARCHAR(255),
	rent_refresh_fee VARCHAR(255),
	guarantee_company TEXT,
	insurance VARCHAR(32),
	current_status VARCHAR(32),
	rent_start_date VARCHAR(32),
	trade_method VARCHAR(32),
	register_date DATE,
	has_elevator BOOLEAN NOT NULL,
	note TEXT,
	has_special_note BOOLEAN NOT NULL
);

CREATE TABLE IF NOT EXISTS lifull_crawler_stats (
	crawl_date DATE NOT NULL,
	category VARCHAR(255) NOT NULL,
	city VARCHAR(255) NOT NULL,
	new_added_house_num INT NOT NULL DEFAULT 0,
	new_unavailable_become_available_house_num INT NOT NULL DEFAULT 0,
	updated_house_num INT NOT NULL DEFAULT 0,
	new_unavailable_house_num INT NOT NULL DEFAULT 0,
	PRIMARY KEY(crawl_date, category, city)
);
