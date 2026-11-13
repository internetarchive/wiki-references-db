-- A read-only role for the API service (wcd-api).
--
--   sudo -u postgres psql -d wiki_references -v pw="$(openssl rand -base64 24)" -f deploy/create_api_role.sql
--
-- then put DATABASE_URL=postgresql://wcd_api:<that password>@127.0.0.1:5432/wiki_references
-- in the service's environment file (deploy/serve.env.example).
--
-- SELECT only, on every table the pipeline role (wcd) owns now and creates later
-- (init-db recreates tables on some re-runs). The timeouts stop one runaway request
-- from holding a backend: the slowest endpoint measured on enwiki takes ~10 s.

CREATE ROLE wcd_api LOGIN PASSWORD :'pw';
GRANT CONNECT ON DATABASE wiki_references TO wcd_api;
GRANT USAGE ON SCHEMA public TO wcd_api;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO wcd_api;
ALTER DEFAULT PRIVILEGES FOR ROLE wcd IN SCHEMA public GRANT SELECT ON TABLES TO wcd_api;
ALTER ROLE wcd_api SET statement_timeout = '60s';
ALTER ROLE wcd_api SET idle_in_transaction_session_timeout = '60s';
ALTER ROLE wcd_api SET default_transaction_read_only = on;
