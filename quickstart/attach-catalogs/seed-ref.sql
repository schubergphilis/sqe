-- Seed the `ref` catalog (a separate warehouse).
CREATE SCHEMA IF NOT EXISTS ref.public;
CREATE TABLE ref.public.regions (region_id BIGINT, name VARCHAR);
INSERT INTO ref.public.regions VALUES (10, 'EU'), (20, 'US');
