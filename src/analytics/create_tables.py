from ..DB_connection import execute_postgre_query

#create date dimension table in PostgreSQL
create_date_dimension_table_query = """
CREATE TABLE IF NOT EXISTS dates (
    date_id INTEGER PRIMARY KEY,
    day INTEGER NOT NULL,
    month INTEGER NOT NULL,
    year INTEGER NOT NULL
);
"""

# create table for address in PostgreSQL
create_address_table_query = """
CREATE TABLE IF NOT EXISTS address (
    id VARCHAR PRIMARY KEY,
    line1 VARCHAR(254),
    city VARCHAR(100),
    state VARCHAR(100),
    country VARCHAR(100),
    status VARCHAR(50)
);
"""

#create vendor dimension table in PostgreSQL
create_vendor_dimension_table_query ="""
CREATE TABLE IF NOT EXISTS vendors (
    id VARCHAR PRIMARY KEY,
    name VARCHAR(254) NOT NULL
);
"""

# create timeline dimension table in PostgreSQL
create_order_timeline_table_query = """
CREATE TABLE IF NOT EXISTS order_timeline (
    id VARCHAR PRIMARY KEY,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    vendor_id VARCHAR,
    date_id INTEGER,
    FOREIGN KEY (vendor_id) REFERENCES vendors(id),
    FOREIGN KEY (date_id) REFERENCES dates(date_id)
);
"""

# create customer dimension table in PostgreSQL
create_customer_dimension_table_query = """
CREATE TABLE IF NOT EXISTS customers (
    id VARCHAR PRIMARY KEY,
    name VARCHAR(254),
    email VARCHAR(254),
    address_id VARCHAR,
    FOREIGN KEY (address_id) REFERENCES address(id)
);
"""

# create product dimension table in PostgreSQL
create_product_dimension_table_query = """
CREATE TABLE IF NOT EXISTS products (
    id VARCHAR PRIMARY KEY,
    name VARCHAR(254),
    price DECIMAL(10, 2)
);
"""

# create orders fact table in PostgreSQL
create_orders_fact_table_query = """
CREATE TABLE IF NOT EXISTS orders(
    id VARCHAR PRIMARY KEY,
    customer_id VARCHAR,
    total_amount DECIMAL(10, 2),
    payment_timeline VARCHAR,
    created_at TIMESTAMP,
    currency VARCHAR(10),
    address_id VARCHAR,
    vendor_id VARCHAR,
    FOREIGN KEY (customer_id) REFERENCES customers(id),
    FOREIGN KEY (vendor_id) REFERENCES vendors(id),
    FOREIGN KEY (payment_timeline) REFERENCES order_timeline(id),
    FOREIGN KEY (address_id) REFERENCES address(id)
);
"""

# create order items table in PostgreSQL
create_order_items_table_query = """
CREATE TABLE IF NOT EXISTS order_items(
    id VARCHAR PRIMARY KEY,
    order_id VARCHAR,
    product_id VARCHAR,
    quantity INTEGER,
    price DECIMAL(10, 2),
    FOREIGN KEY (order_id) REFERENCES orders(id),
    FOREIGN KEY (product_id) REFERENCES products(id)
);
"""

# create order updates table in PostgreSQL
create_order_updates_table_query = """
CREATE TABLE IF NOT EXISTS order_updates(
    id VARCHAR PRIMARY KEY,
    order_id VARCHAR NOT NULL,
    updated_at TIMESTAMP,
    change VARCHAR(500),
    notes VARCHAR(500),
    FOREIGN KEY (order_id) REFERENCES orders(id)
);
"""

# create shipments table in PostgreSQL
create_shipments_table_query = """
CREATE TABLE IF NOT EXISTS shipments(
    id VARCHAR PRIMARY KEY,
    shipment_time TIMESTAMP,
    tracking_id VARCHAR
);
"""

# create shipment updates table in PostgreSQL
create_shipment_updates_table_query = """
CREATE TABLE IF NOT EXISTS shipment_updates(
    id VARCHAR PRIMARY KEY,
    status VARCHAR(50),
    shipment_id VARCHAR,
    updated_at TIMESTAMP,
    order_id VARCHAR,
    FOREIGN KEY (order_id) REFERENCES orders(id),
    FOREIGN KEY (shipment_id) REFERENCES shipments(id)
);
"""

# create refund table in PostgreSQL
create_refunds_table_query = """
CREATE TABLE IF NOT EXISTS refunds(
    id VARCHAR PRIMARY KEY,
    order_id VARCHAR,
    refunded_at TIMESTAMP,
    refund_amount DECIMAL(10, 2),
    currency VARCHAR(10),
    refund_reason VARCHAR(255),
    FOREIGN KEY (order_id) REFERENCES orders(id)
);
"""

#create table for refunded_items in PostgreSQL
create_refunded_items_table_query = """
CREATE TABLE IF NOT EXISTS refund_items(
    id VARCHAR PRIMARY KEY,
    refund_id VARCHAR,
    order_item_id VARCHAR,
    quantity INTEGER,
    amount DECIMAL(10, 2),
    FOREIGN KEY (refund_id) REFERENCES refunds(id),
    FOREIGN KEY (order_item_id) REFERENCES order_items(id)
);
    
"""

#create payments table in PostgreSQL
create_payments_table_query = """
CREATE TABLE IF NOT EXISTS payments (
    id VARCHAR PRIMARY KEY,
    amount DECIMAL NOT NULL,
    currency VARCHAR(10) NOT NULL,
    payment_date TIMESTAMP NOT NULL,
    customer_id VARCHAR NOT NULL,
    order_id VARCHAR NOT NULL,
    status VARCHAR(50) NOT NULL,
    vendor_id VARCHAR NOT NULL,
    payment_method VARCHAR(50),
    FOREIGN KEY (vendor_id) REFERENCES vendors(id),
    FOREIGN KEY (customer_id) REFERENCES customers(id),
    FOREIGN KEY (order_id) REFERENCES orders(id)
);
"""


# List of all create table queries
all_queries_to_execute = [
    create_date_dimension_table_query,
    create_address_table_query,
    create_vendor_dimension_table_query,
    create_order_timeline_table_query,
    create_customer_dimension_table_query,
    create_product_dimension_table_query,
    create_orders_fact_table_query,
    create_order_items_table_query,
    create_order_updates_table_query,
    create_shipments_table_query,
    create_shipment_updates_table_query,
    create_refunds_table_query,
    create_refunded_items_table_query,
    create_payments_table_query,
]

def create_tables_if_not_exists():
    for query in all_queries_to_execute:
        execute_postgre_query(query)
    print("All tables created successfully.")