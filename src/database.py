"""
Author: Thomas Kulch
DS5110 - Final Project - Golf Course Manager

Database Manager
    Used for accessing database throughout the project
"""
import psycopg2
from psycopg2.extras import RealDictCursor
from sqlalchemy import create_engine
import os

class DatabaseManager:
    def __init__(self): # initialize connection to db
        self.connection_params = {
            'host': 'localhost',
            'port': '5432',
            'database': 'golf_analytics',
            'user': 'golf_user',
            'password': 'golf_password'
        }
        self.connection_string = self._get_connection_string()
        self.engine = create_engine(self.connection_string)

    def _get_connection_string(self):
        """get connection string"""
        return f"postgresql://{self.connection_params['user']}:{self.connection_params['password']}@{self.connection_params['host']}:{self.connection_params['port']}/{self.connection_params['database']}"

    def test_connection(self):
        """test database connection"""
        try:
            with psycopg2.connect(**self.connection_params) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT current_database(), version()")
                    result = cur.fetchone()
                    print(f"Connected to database: {result[0]}")
                    return True
        except Exception as e:
            print(f"Database connection failed: {e}")
            return False

    def execute_query(self, query, params=None):
        """Execute query and return results"""
        try:
            with psycopg2.connect(**self.connection_params) as conn:
                # to return query results as dictionary
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute(query, params)
                    # check if it's a SELECT query
                    if query.strip().upper().startswith('SELECT'):
                        return cur.fetchall()  # fetch and return the results
                    else:
                        # For INSERT/UPDATE/DELETE, commit and return affected rows
                        conn.commit()
                        return cur.rowcount
        except Exception as e:
            # rollback on any error
            if conn:
                conn.rollback()
            print(f"Database query error: {e}")
            return None

if __name__ == "__main__":
    # test
    db = DatabaseManager()
    db.test_connection()
    print(db.execute_query("select * from players limit 3"))