CREATE TABLE customer_data (
    customer_id INT PRIMARY KEY,
    customer_name VARCHAR(255) NOT NULL,
    email VARCHAR(255),
    signup_date TIMESTAMP, 
    customer_tenure INT
);

CREATE TABLE sales_data (
    order_id SERIAL PRIMARY KEY,
    customer_id INT NOT NULL,
    product VARCHAR(255),
    quantity INT,
    price NUMERIC(10, 2),
    order_date TIMESTAMP,
    total_value NUMERIC(10, 2),
    customer_name VARCHAR(255),
    email VARCHAR(255),
    order_type VARCHAR(50),
    customer_tenure INT,
    CONSTRAINT fk_customer_id FOREIGN KEY (customer_id) REFERENCES customer_data(customer_id)
);


CREATE TABLE sales_summary (
    product VARCHAR(255) PRIMARY KEY,
    total_sales NUMERIC(10, 2),
    order_count INT
);

-- Index
CREATE INDEX idx_customer_id ON customer_data(customer_id);
CREATE INDEX idx_order_id ON sales_data(order_id);
CREATE INDEX idx_oproduct ON sales_summary(product);