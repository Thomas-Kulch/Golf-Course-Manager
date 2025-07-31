/*
 Author: Thomas Kulch
 DS5110 - Final Project - Golf Course Manager
 Database create script

 Database: golf_analytics - PostgreSQL
 */

-- DROP DATABASE IF EXISTS golf_analytics;

CREATE DATABASE golf_analytics
    WITH
    OWNER = postgres
    ENCODING = 'UTF8'
    LC_COLLATE = 'C'
    LC_CTYPE = 'C'
    LOCALE_PROVIDER = 'libc'
    TABLESPACE = pg_default
    CONNECTION LIMIT = -1
    IS_TEMPLATE = False;

GRANT TEMPORARY, CONNECT ON DATABASE golf_analytics TO PUBLIC;

GRANT ALL ON DATABASE golf_analytics TO golf_user; -- sometimes this doesn't work, use user_permissions.sql queries

GRANT ALL ON DATABASE golf_analytics TO postgres;