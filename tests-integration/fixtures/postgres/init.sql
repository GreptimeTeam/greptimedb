+-- 1. Create a non-superuser
+CREATE USER test_user WITH PASSWORD 'test_password';
+
+-- 2. Prevent the non-superuser from creating tables in the public schema
+-- By default in PostgreSQL 15, non-superusers cannot create tables in the public schema
+-- Explicitly revoke CREATE permission to ensure this
+REVOKE CREATE ON SCHEMA public FROM test_user;

+-- 3. Create a separate schema owned by the non-superuser
+CREATE SCHEMA test_schema AUTHORIZATION test_user;