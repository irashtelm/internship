CREATE SCHEMA datamarts;

CREATE TABLE datamarts.orders_data (
	update_date timestamp default current_timestamp,
	load_id integer,
	transaction_transaction_id text,
	transaction_product_id integer,
	transaction_recorded_on timestamp,
	transaction_quantity numeric,
	transaction_price numeric,
	transaction_price_full numeric,
	transaction_order_type_id text,
	transaction_stores_transaction_id text,
	transaction_stores_pos text,
	stores_pos text,
	stores_pos_name text,
	product_product_id integer,
	product_name_short text,
	product_category_id text,
	product_pricing_line_id numeric,
	product_brand_id integer,
	transaction_recorded_on_cast date,
	stock_available_on date,
	stock_product_id integer,
	stock_pos text,
	stock_available_quantity numeric,
	stock_cost_per_item numeric,
	category_category_id text,
	category_category_name text,
	brand_brand_id integer,
	brand_brand text,
	order_amount numeric,
	order_amount_full_price numeric
);

CREATE TABLE datamarts.stock_data (
	update_date timestamp default current_timestamp,
	load_id integer,
	stock_available_on date,
	stock_product_id integer,
	stock_pos text,
	stock_available_quantity numeric,
	stock_cost_per_item numeric,
	stores_pos text,
	stores_pos_name text,
	product_product_id integer,
	product_name_short text,
	product_category_id text,
	product_pricing_line_id numeric,
	product_brand_id integer,
	category_category_id text,
	category_category_name text,
	brand_brand_id integer,
	brand_brand text,
	product_quantity_product_id integer,
	product_quantity_min_quantity numeric,
	stores_emails_pos text,
	stores_emails_email text,
	available_amount numeric

);

CREATE TABLE datamarts.stores_data (
	update_date timestamp DEFAULT CURRENT_TIMESTAMP,
	load_id integer,
	stores_pos text,
	stores_pos_name text,
	stores_emails_email text,
	product_product_id integer,
	product_name_short text,
	category_category_id text,
	stock_available_on date,
	stock_available_quantity numeric,
	product_quantity_min_quantity numeric
);

