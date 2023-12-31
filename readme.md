## Examples on how to load datasets using DuckDB Python API

To run the examples, install environment using conda:
```
conda env create -f environment.yml
```
Activate the environment:
```
conda activate duckdb-examples
```

### Load from Parquet into DuckDB

Run the examples:
```
python Parquet/read_from_parquet.py
```

### Load from PostgreSQL into DuckDB
Install DuckDB PostgreSQL extension:
```
python -c "import duckdb; duckdb.execute('INSTALL postgres;')"
```
Set PostgreSQL connection in `PostgreSQL/connection.yml` <br>
If needed, create an example table:
```
python PostgreSQL/create_example_table_PostgreSQL.py
```
Run the examples:
```
python PostgreSQL/read_from_postgresql.py
```
