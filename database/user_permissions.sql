/*
 Author: Thomas Kulch
 DS5110 - Final Project - Golf Course Manager

 Golf Course Manager Database user permissions
 Use this if your golf_user does not have full access to db
 */

--CREATE USER golf_user WITH PASSWORD 'golf_password';
GRANT ALL PRIVILEGES ON DATABASE golf_analytics TO golf_user;
-- Grant all privileges on the database
GRANT ALL PRIVILEGES ON DATABASE golf_analytics TO golf_user;

-- Grant all privileges on all existing tables
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO golf_user;

-- Grant all privileges on all existing sequences
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO golf_user;

-- Grant privileges on future tables
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO golf_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO golf_user;