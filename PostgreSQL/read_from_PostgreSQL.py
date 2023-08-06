import duckdb

duckdb.execute("LOAD postgres")

host: str = "hostname"
database_name: str = "postgresql-db"
user: str = "user-example"
password: str = "password"
port: int = 5432

source_schema: str = "public"  # schema of the postgres database to load from
sink_schema: str = "public"   # schema name to use to create views in DuckDB

duckdb.execute(  # register postgreSQL tables as views in DuckDB
    f"CALL POSTGRES_ATTACH('dbname={database_name}, host={host}, port={port}, "
    f"user={user}, password={password}', "
    f"source_schema={source_schema}, sink_schema={sink_schema});"
)

# Show all tables and views in DuckDB
duckdb.execute("SELECT * FROM duckdb_tables;").show()
duckdb.execute("PRAGMA show_tables;").show()
