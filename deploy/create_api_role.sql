-- A read-only role for the API service (wcd-api).
--
-- From the serving checkout; the password goes straight into serve.env (mode 600), never
-- onto the screen (hex: no characters that need escaping in DATABASE_URL):
--
--   umask 077 && PW=$(openssl rand -hex 24) \
--     && sudo -u postgres psql -d wiki_references -v pw="$PW" -f deploy/create_api_role.sql \
--     && sed "s|CHANGE_ME|$PW|" deploy/serve.env.example > serve.env && unset PW
--
-- To change the password later: the same, with  -c "ALTER ROLE wcd_api PASSWORD '$PW'"
-- in place of  -v pw=... -f deploy/create_api_role.sql.
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
