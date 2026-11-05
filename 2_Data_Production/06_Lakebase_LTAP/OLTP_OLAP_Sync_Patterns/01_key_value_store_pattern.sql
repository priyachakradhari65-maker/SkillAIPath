-- Databricks notebook source
-- Key-Value Store (Redis Replacement)
CREATE TABLE sap_oltp.cache_store (
    key STRING PRIMARY KEY,
    value STRING,
    ttl_seconds INT,
    created_at TIMESTAMP
)
COMMENT "Key-value store pattern for fast lookups by key";

-- Fast writes (shopping cart, session data)
INSERT INTO sap_oltp.cache_store VALUES
('cart_user123', '{"items": [1,2,3]}', 3600, CURRENT_TIMESTAMP());

-- Fast reads
SELECT value FROM sap_oltp.cache_store WHERE key = 'cart_user123';