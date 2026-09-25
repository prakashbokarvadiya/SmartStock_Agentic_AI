-- Products table
CREATE TABLE IF NOT EXISTS products (
    product_id VARCHAR(10) PRIMARY KEY,
    product_name VARCHAR(150) NOT NULL,
    category VARCHAR(80) NOT NULL,
    brand VARCHAR(80) NOT NULL,
    price DECIMAL(12, 2) NOT NULL CHECK (price >= 0),
    stock INT NOT NULL CHECK (stock >= 0)
);

-- Orders table
CREATE TABLE IF NOT EXISTS orders (
    order_id VARCHAR(10) PRIMARY KEY,
    customer_id VARCHAR(10) NOT NULL,
    product_id VARCHAR(10) NOT NULL,
    quantity INT NOT NULL CHECK (quantity > 0),
    order_date DATE NOT NULL,
    order_status VARCHAR(20) NOT NULL CHECK (order_status IN ('Confirmed','Processing','Shipped','Delivered','Returned','Cancelled')),
    CONSTRAINT fk_orders_product FOREIGN KEY (product_id) REFERENCES products(product_id)
        ON UPDATE CASCADE ON DELETE RESTRICT
);

-- Inventory Alerts table
CREATE TABLE IF NOT EXISTS inventory_alerts (
    id SERIAL PRIMARY KEY,
    product_id VARCHAR(10) NOT NULL,
    alert_type VARCHAR(50) NOT NULL,
    stock_level INT NOT NULL,
    alert_sent_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_product_alert UNIQUE (product_id, alert_type),
    CONSTRAINT fk_inventory_alert_product FOREIGN KEY (product_id) REFERENCES products(product_id) ON DELETE CASCADE
);

-- Sample product rows
INSERT INTO products (product_id, product_name, category, brand, price, stock) VALUES
('P001','Samsung Smart TV','Television','Samsung',45000,15),
('P002','LG Double Door Fridge','Refrigerator','LG',38000,10),
('P003','Redmi Note Mobile','Smartphone','Xiaomi',18000,30),
('P004','Samsung Full HD LED TV','Television','Samsung',30956,33),
('P005','LG UHD Smart TV','Television','LG',32695,40),
('P006','LG OLED evo TV','Television','LG',34434,47),
('P007','LG NanoCell TV','Television','LG',36173,8),
('P008','LG Full HD Smart TV','Television','LG',37912,15),
('P009','Sony BRAVIA 4K TV','Television','Sony',39651,22)
ON CONFLICT (product_id) DO UPDATE SET 
    product_name = EXCLUDED.product_name, 
    category = EXCLUDED.category, 
    brand = EXCLUDED.brand, 
    price = EXCLUDED.price, 
    stock = EXCLUDED.stock;

-- Sample order rows
INSERT INTO orders (order_id, customer_id, product_id, quantity, order_date, order_status) VALUES
('O1001','C101','P001',1,'2026-08-10','Delivered'),
('O1002','C102','P003',2,'2026-08-15','Shipped'),
('O1003','C103','P002',1,'2026-08-20','Processing'),
('O1006','C119','P003',3,'2026-06-15','Delivered')
ON CONFLICT (order_id) DO UPDATE SET 
    customer_id = EXCLUDED.customer_id, 
    product_id = EXCLUDED.product_id, 
    quantity = EXCLUDED.quantity, 
    order_date = EXCLUDED.order_date, 
    order_status = EXCLUDED.order_status;
