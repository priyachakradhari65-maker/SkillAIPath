-- Lakehouse Federation: MySQL Connection
-- Connect to external MySQL database without ETL

CREATE CONNECTION IF NOT EXISTS mysql_legacy
TYPE MYSQL
OPTIONS (
  host 'legacy-db.example.com',
  port '3306',
  user secret('mysql_scope', 'username'),
  password secret('mysql_scope', 'password')
);

CREATE FOREIGN CATALOG IF NOT EXISTS mysql_catalog
USING CONNECTION mysql_legacy
OPTIONS (database 'production');

-- Query external MySQL table directly
SELECT * FROM mysql_catalog.production.customers LIMIT 10;
