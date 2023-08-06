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

sink_schema: str = "dbschema"   # schema name to use to create views in DuckDB

duckdb.execute(  # register postgreSQL tables as views in DuckDB
    f"CALL POSTGRES_ATTACH('dbname={database_name} host={host} port={port} "
    f"user={user} password={password}', "
    f"source_schema={source_schema}, sink_schema={sink_schema});"
)

# Show all tables and views in DuckDB
duckdb.sql("SELECT * FROM duckdb_tables").show()
duckdb.sql("PRAGMA show_tables").show()

