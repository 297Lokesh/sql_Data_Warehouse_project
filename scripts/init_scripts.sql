-- =====================================================
-- SUMMARY:
-- In MySQL, SCHEMA and DATABASE are equivalent.
-- This script creates a data warehouse structure
-- using Bronze, Silver, and Gold layers.
--
-- NOTE:
-- These layers will be created as separate databases,
-- not as schemas inside a single database.
-- =====================================================


-- =====================================================
-- PURPOSE:
-- To organize data into three logical layers:
--
-- Bronze → Stores raw data (unprocessed)
-- Silver → Stores cleaned and transformed data
-- Gold   → Stores final data for reporting/analysis
-- =====================================================


-- =====================================================
-- STEP 1: Create main database (if not exists)
-- This step is optional in this design
-- =====================================================
CREATE DATABASE IF NOT EXISTS DataWarehouse;


-- =====================================================
-- STEP 2: Select the main database
-- =====================================================
USE DataWarehouse;


-- =====================================================
-- STEP 3: Create Bronze layer (raw data)
-- =====================================================
CREATE SCHEMA IF NOT EXISTS bronze;


-- =====================================================
-- STEP 4: Create Silver layer (cleaned data)
-- =====================================================
CREATE SCHEMA IF NOT EXISTS silver;


-- =====================================================
-- STEP 5: Create Gold layer (final data)
-- =====================================================
CREATE SCHEMA IF NOT EXISTS gold;