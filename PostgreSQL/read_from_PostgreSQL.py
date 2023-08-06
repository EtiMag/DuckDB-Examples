import os
import yaml
import duckdb

duckdb.execute("LOAD postgres")

with open(f"{os.path.dirname(__file__)}/connection.yml", "r") as connection_file:
    connection_parameters = yaml.safe_load(connection_file)
    host: str = connection_parameters["host"]
    database_name: str = connection_parameters["database_name"]
    user: str = connection_parameters["user"]
    password: str = connection_parameters["password"]
    port: int = connection_parameters["port"]
    source_schema: str = connection_parameters["source_schema"]


# register postgreSQL tables as views in DuckDB
# from schema source_schema, into schema sink_schema
sink_schema: str = "new_schema"
duckdb.execute(f"CREATE SCHEMA IF NOT EXISTS {sink_schema}")

duckdb.execute(
    f"CALL POSTGRES_ATTACH('dbname={database_name} host={host} port={port} "
    f"user={user} password={password}', "
    f"source_schema={source_schema}, sink_schema={sink_schema}, overwrite=True)"
)  # overwrite = True means that existing views in schema sink_schema should be overwritten

# Show all tables and views in DuckDB

duckdb.sql("SELECT * FROM information_schema.tables").show()
