-- =============================================================================
-- Test Data Setup for the Generalized Healing Agent
-- =============================================================================
-- Creates and seeds three raw tables in DATA_OPS_DEMO.RAW:
--   RAW_CUSTOMERS  - customer master data
--   RAW_ORDERS     - orders placed by customers
--   RAW_PRODUCTS   - product catalog
--
-- A dbt model (fct_order_summary.sql) joins all three. The broken-model
-- scenarios under dbt_project/models/broken_scenarios/ intentionally fail
-- against this data so the agent's Audit Workflow can be exercised.
--
-- Run this in a Snowflake worksheet (or via SnowSQL) before testing.
-- =============================================================================

USE DATABASE DATA_OPS_DEMO;
USE SCHEMA RAW;
USE WAREHOUSE COMPUTE_WH;

-- -----------------------------------------------------------------------------
-- RAW_CUSTOMERS
-- Note: the real column is CUST_ID (not CUSTOMER_ID) — this mismatch is used
-- by Scenario 1.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE RAW_CUSTOMERS (
    CUST_ID      STRING,
    FIRST_NAME   STRING,
    LAST_NAME    STRING,
    EMAIL        STRING,
    CREATED_AT   TIMESTAMP_NTZ
);

INSERT INTO RAW_CUSTOMERS (CUST_ID, FIRST_NAME, LAST_NAME, EMAIL, CREATED_AT) VALUES
    ('CUST-001', 'Alice',  'Johnson', 'alice.johnson@example.com', '2024-01-05 09:30:00'),
    ('CUST-002', 'Bob',    'Smith',   'bob.smith@example.com',     '2024-01-06 11:15:00'),
    ('CUST-003', 'Carol',  'White',   'carol.w@example.com',       '2024-01-07 14:45:00'),
    ('CUST-004', 'David',  'Brown',   'david.brown@example.com',   '2024-01-08 08:05:00'),
    ('CUST-005', 'Eva',    'Martins', 'eva.m@example.com',         '2024-01-09 16:20:00');

-- -----------------------------------------------------------------------------
-- RAW_PRODUCTS
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE RAW_PRODUCTS (
    PRODUCT_ID    STRING,
    PRODUCT_NAME  STRING,
    CATEGORY      STRING,
    UNIT_PRICE    NUMBER(10,2)
);

INSERT INTO RAW_PRODUCTS (PRODUCT_ID, PRODUCT_NAME, CATEGORY, UNIT_PRICE) VALUES
    ('PROD-100', 'Wireless Mouse',    'Accessories', 25.50),
    ('PROD-101', 'Mechanical Keyboard','Accessories', 89.99),
    ('PROD-102', 'USB-C Hub',          'Accessories', 42.00),
    ('PROD-103', '27-inch Monitor',    'Displays',    310.75),
    ('PROD-104', 'Laptop Stand',       'Accessories', 34.25);

-- -----------------------------------------------------------------------------
-- RAW_ORDERS
-- Note: ORDER_AMOUNT is stored as STRING (dirty data) on purpose — this feeds
-- the invalid-cast scenario (Scenario 3). ORDER_DATE is also a STRING.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE RAW_ORDERS (
    ORDER_ID       STRING,
    CUST_ID        STRING,
    PRODUCT_ID     STRING,
    QUANTITY       NUMBER(10,0),
    ORDER_AMOUNT   STRING,       -- intentionally STRING (e.g. '125.50', 'N/A')
    ORDER_DATE     STRING        -- intentionally STRING (e.g. '2024-02-01')
);

INSERT INTO RAW_ORDERS (ORDER_ID, CUST_ID, PRODUCT_ID, QUANTITY, ORDER_AMOUNT, ORDER_DATE) VALUES
    ('ORD-9001', 'CUST-001', 'PROD-100', 2, '51.00',  '2024-02-01'),
    ('ORD-9002', 'CUST-002', 'PROD-103', 1, '310.75', '2024-02-03'),
    ('ORD-9003', 'CUST-001', 'PROD-101', 1, '89.99',  '2024-02-05'),
    ('ORD-9004', 'CUST-003', 'PROD-102', 3, '126.00', '2024-02-06'),
    ('ORD-9005', 'CUST-004', 'PROD-104', 2, '68.50',  '2024-02-08'),
    ('ORD-9006', 'CUST-005', 'PROD-100', 1, '25.50',  '2024-02-10');

-- -----------------------------------------------------------------------------
-- Quick sanity checks
-- -----------------------------------------------------------------------------
SELECT 'RAW_CUSTOMERS' AS tbl, COUNT(*) AS n FROM RAW_CUSTOMERS
UNION ALL SELECT 'RAW_PRODUCTS', COUNT(*) FROM RAW_PRODUCTS
UNION ALL SELECT 'RAW_ORDERS',   COUNT(*) FROM RAW_ORDERS;
