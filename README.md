# flow-of-flows
Demonstrates how to manage codependent flows in Prefect Core

# Run

pip install -e .

export PREFECT__CONTEXT__SECRETS__POSTGRES_USER=postgres
export PREFECT__CONTEXT__SECRETS__POSTGRES_PASS=your_password_1234
export PREFECT__CONTEXT__SECRETS__GITHUB_ACCESS_TOKEN=

docker run -d --name demo_postgres -v dbdata:/var/lib/postgresql/data -p 5433:5432 \
-e POSTGRES_PASSWORD=your_password_1234 postgres:11

docker run -d --name admin_postgres -p 8081:8080 adminer

prefect server start

prefect agent local start --label dev --no-hostname-label -p ~/nextail/repositorios/desarrollos/prefect/flow-of-flows

prefect create project jaffle_shop

prefect register --project jaffle_shop -p flows/01_extract_load.py

prefect run --name 01_extract_load --watch

psql postgresql://postgres:your_password_1234@localhost:5433/postgres
SELECT *
FROM pg_catalog.pg_tables
WHERE schemaname != 'pg_catalog' AND 
    schemaname != 'information_schema';

cp profiles.yml to .dbt on local home

prefect register --project jaffle_shop -p flows/02_dbt.py

prefect run --name 02_dbt --watch

prefect register --project jaffle_shop -p flows/03_dashboards.py

prefect run --name 03_dashboards --watch

prefect register --project jaffle_shop -p flows/04_orchestrating_flow.py

prefect run --name 04_orchestrating_flow --watch