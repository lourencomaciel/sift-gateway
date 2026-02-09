-- Postgres init script: creates the integration-test database.
-- Mounted at /docker-entrypoint-initdb.d/ in docker-compose.yml and
-- executed automatically on first container start.
--
-- The test DB is owned by the same mcp_gateway role that docker-compose
-- creates via POSTGRES_USER, so no extra roles are needed.

CREATE DATABASE mcp_test OWNER mcp_gateway;
