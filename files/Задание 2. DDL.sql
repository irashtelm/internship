

CREATE TABLE dds.brand (
    brand_id integer PRIMARY KEY,
    brand text NOT NULL
);

CREATE TABLE dds.category (
	category_id text PRIMARY KEY,
	category_name text NOT NULL
);

CREATE TABLE dds.product (
	product_id integer PRIMARY KEY,
	name_short text NOT NULL,
	category_id text NOT NULL,
	pricing_line_id integer NULL,
	brand_id integer NOT NULL,
	CONSTRAINT fk_product_category FOREIGN KEY (category_id) REFERENCES category (category_id),
	CONSTRAINT fk_product_brand FOREIGN KEY (brand_id) REFERENCES brand (brand_id)
);

CREATE TABLE dds.stock (
	available_on date,
	product_id integer,
	pos text,
	available_quantity real NOT NULL CHECK (available_quantity >= 0),
	cost_per_item real NOT NULL CHECK (cost_per_item >= 0),
	CONSTRAINT pk_stock PRIMARY KEY (available_on, product_id, pos),
	CONSTRAINT fk_stock_product FOREIGN KEY (product_id) REFERENCES product (product_id)
);

CREATE TABLE dds."transaction" (
	transaction_id text,
	product_id integer,
	recorded_on timestamp NOT NULL,
	quantity real NOT NULL CHECK (quantity >= 1),
	price real NOT NULL CHECK (price >= 0),
	price_full real NOT NULL CHECK (price_full > 0),
	order_type_id text NOT NULL,
	CONSTRAINT pk_transaction PRIMARY KEY (transaction_id, product_id),
	CONSTRAINT fk_transaction_product FOREIGN KEY (product_id) REFERENCES product (product_id)
);

