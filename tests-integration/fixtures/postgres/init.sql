-- 1. Create a non-superuser
CREATE USER test_user WITH PASSWORD 'test_password';

-- 2. Prevent non-superusers from creating tables in the public schema
-- Explicitly revoke CREATE permission from PUBLIC to avoid inherited grants
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
REVOKE CREATE ON SCHEMA public FROM test_user;

-- 3. Create a separate schema owned by the non-superuser
CREATE SCHEMA test_schema AUTHORIZATION test_user;
