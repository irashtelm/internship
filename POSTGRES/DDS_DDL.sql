CREATE SCHEMA dds;

CREATE TABLE dds.brand (
    brand_id integer PRIMARY KEY,
    brand text NOT NULL
);

CREATE TABLE dds.category (
	category_id text PRIMARY KEY,
	category_name text NOT NULL
);

CREATE TABLE dds.stores (
	pos text PRIMARY KEY,
	pos_name text NOT NULL
);

CREATE TABLE dds.product (
	product_id integer PRIMARY KEY,
	name_short text NOT NULL,
	category_id text NOT NULL,
	pricing_line_id integer,
	brand_id integer NOT NULL,
	CONSTRAINT fk_product_category FOREIGN KEY (category_id) REFERENCES category (category_id),
	CONSTRAINT fk_product_brand FOREIGN KEY (brand_id) REFERENCES brand (brand_id)
);

CREATE TABLE dds.transaction_stores (
	transaction_id text PRIMARY KEY,
	pos text NOT NULL
);

CREATE TABLE dds.stock (
	available_on date,
	product_id integer,
	pos text,
	available_quantity numeric NOT NULL CHECK (available_quantity >= 0),
	cost_per_item numeric NOT NULL CHECK (cost_per_item >= 0),
	CONSTRAINT pk_stock PRIMARY KEY (available_on, product_id, pos),
	CONSTRAINT fk_stock_product FOREIGN KEY (product_id) REFERENCES product (product_id)
);

CREATE TABLE dds.transaction (
	transaction_id text,
	product_id integer,
	recorded_on timestamp NOT NULL,
	quantity numeric NOT NULL CHECK (quantity >= 1),
	price numeric NOT NULL CHECK (price >= 0),
	price_full numeric NOT NULL CHECK (price_full > 0),
	order_type_id text,
	CONSTRAINT pk_transaction PRIMARY KEY (transaction_id, product_id),
	CONSTRAINT fk_transaction_product FOREIGN KEY (product_id) REFERENCES product (product_id)
);

-- Создание журналов событий
CREATE SCHEMA data_quality;
CREATE TABLE data_quality.brand_log (
	brand_id text,
	brand text,
	noises text,
	missing_values_check text,
	duplicates_check text,
	data_types_check text,
	value_restrict_check text,
	len_restrict_check text,
	ref_integrity_check text
);

CREATE TABLE data_quality.category_log  (
	category_id text,
	category_name text,
	noises text,
	missing_values_check text,
	duplicates_check text,
	data_types_check text,
	value_restrict_check text,
	len_restrict_check text,
	ref_integrity_check text
);

CREATE TABLE data_quality.stores_log  (
	pos text,
	pos_name text,
	noises text,
	missing_values_check text,
	duplicates_check text,
	data_types_check text,
	value_restrict_check text,
	len_restrict_check text,
	ref_integrity_check text
);

CREATE TABLE data_quality.product_log (
	product_id text,
	name_short text,
	category_id text,
	pricing_line_id text,
	brand_id text,
	noises text,
	missing_values_check text,
	duplicates_check text,
	data_types_check text,
	value_restrict_check text,
	len_restrict_check text,
	ref_integrity_check text
);

CREATE TABLE data_quality.transaction_stores_log (
	transaction_id text,
	pos text,
	noises text,
	missing_values_check text,
	duplicates_check text,
	data_types_check text,
	value_restrict_check text,
	len_restrict_check text,
	ref_integrity_check text
);

CREATE TABLE data_quality.stock_log (
	available_on text,
	product_id text,
	pos text,
	available_quantity text,
	cost_per_item text,
	noises text,
	missing_values_check text,
	duplicates_check text,
	data_types_check text,
	value_restrict_check text,
	len_restrict_check text,
	ref_integrity_check text
);

CREATE TABLE data_quality.transaction_log (
	transaction_id text,
	product_id text,
	recorded_on text,
	quantity text,
	price text ,
	price_full text,
	order_type_id text,
	noises text,
	missing_values_check text,
	duplicates_check text,
	data_types_check text,
	value_restrict_check text,
	len_restrict_check text,
	ref_integrity_check text
);

CREATE TABLE data_quality.upload_tables_tech (
	table_name text,
	update_date timestamp,
	load_id integer
);