import os
import yaml
import sqlalchemy

with open(f"{os.path.dirname(__file__)}/connection.yml", "r") as connection_file:
    connection_parameters = yaml.safe_load(connection_file)
    host: str = connection_parameters["host"]
    database_name: str = connection_parameters["database_name"]
    user: str = connection_parameters["user"]
    password: str = connection_parameters["password"]
    port: int = connection_parameters["port"]
    source_schema: str = connection_parameters["source_schema"]


engine = sqlalchemy.create_engine(f"postgresql://{user}:{password}@{host}:{port}/{database_name}")

# Create table example_table with some values, in schema [source_schema]
with engine.begin() as connection:
    connection.execute(
        sqlalchemy.text(f"CREATE TABLE {source_schema}.example_table "
                         "(col1 INT, col2 INT, col3 INT)")
    )
    connection.execute(
        sqlalchemy.text(f"INSERT INTO {source_schema}.example_table "
                         "VALUES (1, 2, 3), (4, 5, 6), (7, 8, 9)")
    )
