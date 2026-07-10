-- Seed the `sales` catalog (its own SQLite-backed warehouse).
CREATE SCHEMA IF NOT EXISTS sales.public;
CREATE TABLE sales.public.orders (id BIGINT, region_id BIGINT, amount DOUBLE);
INSERT INTO sales.public.orders VALUES (1, 10, 42.00), (2, 20, 13.50), (3, 10, 7.25);
