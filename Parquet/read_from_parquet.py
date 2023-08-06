import duckdb
import pandas as pd

# directly read the table
duckdb.sql(query="SELECT * FROM iris.parquet").show()

# Load iris.parquet directly into memory database
con: duckdb.DuckDBPyConnection = duckdb.connect()
duckdb.sql(query="CREATE TABLE iris_direct_load AS SELECT * FROM parquet_scan('iris.parquet')",
           connection=con)

duckdb.sql("SELECT * FROM iris_direct_load", connection=con).show()


# Create view and load from it
df_iris: pd.DataFrame = pd.read_parquet("iris.parquet")
duckdb.register(view_name="view_iris_parquet",
                python_object=df_iris,
                connection=con)

duckdb.sql(query="CREATE TABLE iris_load_from_view AS SELECT * FROM view_iris_parquet",
           connection=con)
duckdb.sql(query="SELECT * FROM iris_load_from_view",connection=con).show()
duckdb.unregister("view_iris_parquet", connection=con)

# Same with another syntax
duckdb.sql(query="CREATE VIEW view_iris_parquet AS SELECT * FROM parquet_scan('iris.parquet')",
           connection=con)
duckdb.sql(query="CREATE TABLE iris_load_from_view_2 AS SELECT * FROM view_iris_parquet",
           connection=con)
duckdb.sql(query="SELECT * FROM iris_load_from_view_2",connection=con).show()

# Directly load from python object

duckdb.sql("CREATE TABLE iris_load_from_python_object AS SELECT * FROM df_iris", connection=con)
duckdb.sql("SELECT * FROM iris_load_from_python_object", connection=con).show()
