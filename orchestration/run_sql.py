"""
Executes SQL scripts or commands against the Hive Metastore using PySpark.
Replaces the need for 'beeline' in Airflow containers.
"""

import sys
import argparse
from processing.spark_config import get_spark_session


def run_sql(query: str = None, file_path: str = None):
    spark = get_spark_session("Airflow-SQL-Runner")
    
    queries = []
    if file_path:
        with open(file_path, "r") as f:
            content = f.read()
            # Split by semicolon, ignore empty statements
            queries = [q.strip() for q in content.split(";") if q.strip()]
    elif query:
        queries = [query.strip()]
    else:
        print("Error: Must provide either --query or --file")
        sys.exit(1)

    for q in queries:
        print(f"Executing: {q}")
        try:
            spark.sql(q)
            print("Success.")
        except Exception as e:
            print(f"Error executing query: {e}")
            sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Spark SQL queries")
    parser.add_argument("--query", "-q", type=str, help="Single SQL query to execute")
    parser.add_argument("--file", "-f", type=str, help="Path to a .sql file to execute")
    args = parser.parse_args()
    
    run_sql(query=args.query, file_path=args.file)
