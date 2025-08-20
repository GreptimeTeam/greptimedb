-- Disable CREATE in public schema to simulate PostgreSQL 15+ restrictive default
ALTER SCHEMA public OWNER TO postgres;
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
REVOKE ALL ON SCHEMA public FROM PUBLIC;

-- Create a dedicated schema for tests and grant privileges to the app user
DO $$
BEGIN
   IF NOT EXISTS (
      SELECT 1 FROM pg_namespace WHERE nspname = 'greptime_schema'
   ) THEN
      EXECUTE 'CREATE SCHEMA greptime_schema AUTHORIZATION greptimedb';
   END IF;
END
$$;

GRANT USAGE, CREATE ON SCHEMA greptime_schema TO greptimedb;
