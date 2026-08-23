CREATE TABLE IF NOT EXISTS public.customers (
    customer_id INT PRIMARY KEY,
    name TEXT,
    email TEXT
);

CREATE TABLE IF NOT EXISTS public.orders (
    order_id INT PRIMARY KEY,
    customer_id INT,
    product_id INT,
    amount NUMERIC
);

INSERT INTO public.customers VALUES
(1, 'Alice', 'alice@example.com'),
(2, 'Bob', 'bob@example.com')
ON CONFLICT DO NOTHING;

INSERT INTO public.orders VALUES
(100, 1, 10, 49.99),
(101, 2, 11, 19.99)
ON CONFLICT DO NOTHING;