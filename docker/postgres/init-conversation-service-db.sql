-- Creates the conversation-service database on the FIRST init of the shared graphgpt-postgres volume
-- (docker-entrypoint-initdb.d only runs against an empty data directory). On an existing volume use
--   make -C conversation-service db-create
SELECT 'CREATE DATABASE conversation_service_db'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'conversation_service_db')\gexec
