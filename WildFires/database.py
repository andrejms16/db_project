import pandas as pd
import psycopg2, pandas
import warnings
from tabulate import tabulate


class PostgresSQL:
    def __init__(self, host, port, database, user, password, schema):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.schema = schema
        try:
            self.connection = psycopg2.connect(
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.user,
            password=password
            )
            self.cursor = self.connection.cursor()
            self.cursor.execute(f"SET search_path TO {self.schema};")
        except psycopg2.DatabaseError as e:
            print('Error while connecting to PostgresSQL', e)
            self.connection = None

    def test_connection(self):
        self.cursor.execute('SELECT version();')
        record = self.cursor.fetchone()
        print("Connected to PostgresSQL!")
        print('PostgresSQL version:', record)

    def insert_data(self, table_name, data):
        columns = ', '.join(data.keys())
        placeholders = ', '.join(['%s'] * len(data))
        query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query, tuple(data.values()))
                self.connection.commit()
                return True
        except Exception as e:
            self.connection.rollback()
            print('Error inserting new data: ',e)
            return None

    def update_data(self, table_name, data, condition):
        set_clause = ', '.join([f"{k} = %s" for k in data.keys()])
        where_clause = ' AND '.join([f"{k} = %s" for k in condition.keys()])
        query = f"UPDATE {table_name} SET {set_clause} WHERE {where_clause};"
        try:
            with self.connection.cursor() as cur:
                cur.execute(query, tuple(data.values()) + tuple(condition.values()))
                self.connection.commit()
                return True
        except Exception as e:
            self.connection.rollback()
            print("Error updating data:", e)
            return None

    def is_foreign_key(self, table_name, column_name):
        try:
            query = f"""SELECT
                        kcu.column_name AS foreign_key_column,
                        ccu.table_name AS referenced_table,
                        ccu.column_name AS referenced_column
                        FROM
                        information_schema.key_column_usage AS kcu
                        JOIN
                        information_schema.referential_constraints AS rc
                        ON kcu.constraint_name = rc.constraint_name
                        AND kcu.table_schema = rc.constraint_schema
                        JOIN
                        information_schema.constraint_column_usage AS ccu
                        ON ccu.constraint_name = rc.unique_constraint_name
                        AND ccu.table_schema = rc.unique_constraint_schema
                        WHERE
                        kcu.table_name = '{table_name}'
                        AND kcu.column_name = '{column_name}';"""
            self.cursor.execute(query)
            result = self.cursor.fetchone()
            if result:
                return {'referenced_table': result[1], 'referenced_column': result[2]}
            else:
                return False
        except Exception as e:
            print('Error checking for foreign keys: ',e)
            return None

    def is_primary_key(self, table_name, column_name):
        try:
            query = f"""SELECT
                        kcu.column_name AS primary_key_column
                        FROM
                        information_schema.table_constraints AS tc
                        JOIN
                        information_schema.key_column_usage AS kcu
                        ON tc.constraint_name = kcu.constraint_name
                        AND tc.table_schema = kcu.table_schema
                        WHERE
                        tc.constraint_type = 'PRIMARY KEY'
                        AND kcu.table_name = '{table_name}'
                        AND kcu.column_name = '{column_name}';"""
            self.cursor.execute(query)
            result = self.cursor.fetchone()
            if result:
                return {'primary_key': result[0]}
            else:
                return False
        except Exception as e:
            print('Error checking for primary key: ',e)
            return None

    def is_serial(self, table_name, column_name):
        try:
            query = f"SELECT pg_get_serial_sequence('{table_name}', '{column_name}') IS NOT NULL AS is_serial;"
            self.cursor.execute(query)
            return self.cursor.fetchone()[0]
        except Exception as e:
            print('Error checking for serial columns: ',e)
            return None

    def get_columns(self, table_name):
        try:
            query = f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table_name}';"
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except Exception as e:
            print('Error getting columns: ',e)
            return None

    def query_one(self, table_name, condition):
        warnings.filterwarnings("ignore", category=UserWarning, message="pandas only supports SQLAlchemy connectable")
        try:
            query = f"SELECT * FROM {table_name} WHERE {condition[0]} = '{condition[1]}';"
            df = pd.read_sql_query(query, con=self.connection)
            print(tabulate(df, headers='keys', tablefmt='psql'))
            return True
        except Exception as e:
            print('Error querying data: ',e)
            return False

    def custom_query(self, query, screenshot):
        warnings.filterwarnings("ignore", category=UserWarning, message="pandas only supports SQLAlchemy connectable")
        try:
            df = pd.read_sql_query(query, con=self.connection)
            if screenshot:
                print(tabulate(df, headers='keys', tablefmt='psql'))
            return df
        except Exception as e:
            print('Error querying data: ',e)
            return False

    def delete_one(self, tabel_name, condition):
        try:
            query = f"DELETE FROM {tabel_name} WHERE {condition[0]} = '{condition[1]}';"
            self.cursor.execute(query)
            self.connection.commit()
            return True
        except Exception as e:
            print('Error deleting data: ',e)
            return False

    def close_connection(self):
        self.connection.close()





