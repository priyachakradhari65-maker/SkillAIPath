-- PostgreSQL Foreign Catalog Setup
CREATE CONNECTION IF NOT EXISTS postgres_legacy
TYPE POSTGRESQL
OPTIONS (
  host 'postgres.example.com',
  port '5432',
  user secret('postgres_scope', 'username'),
  password secret('postgres_scope', 'password')
);

CREATE FOREIGN CATALOG IF NOT EXISTS postgres_catalog
USING CONNECTION postgres_legacy;

-- Test connection
SELECT COUNT(*) FROM postgres_catalog.public.orders;
