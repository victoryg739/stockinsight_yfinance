import psycopg2

class DatabaseHandler:
    def __init__(self):
        self.db_params = {
            'dbname': 'verceldb',
            'user': 'default',
            'password': 'bsCa5Up6lhTG',
            'host': 'ep-noisy-waterfall-a1jfpfg9.ap-southeast-1.aws.neon.tech',
            'port': '5432'
        }
        self.conn = None
        self.cur = None

    def connect(self):
        """Establish a connection to the PostgreSQL database."""
        try:
            self.conn = psycopg2.connect(**self.db_params)
            self.cur = self.conn.cursor()
            print("Database connection established.")
        except Exception as e:
            print(f"An error occurred while connecting to the database: {e}")
            self.conn = None
            self.cur = None

    def execute_query(self, query, params=None):
        """Execute a query and commit the transaction."""
        try:
            if params:
                self.cur.execute(query, params)
            else:
                self.cur.execute(query)
            self.conn.commit()
        except Exception as e:
            print(f"An error occurred: {e}")
            self.conn.rollback()

    def execute_query_many(self, query, data):
        """Execute a query with multiple sets of parameters."""
        try:
            self.cur.executemany(query, data)
            self.conn.commit()
        except Exception as e:
            print(f"An error occurred: {e}")
            self.conn.rollback()

    def fetch_query(self, query, params=None):
        """Execute a SELECT query and return the results."""
        try:
            if params:
                self.cur.execute(query, params)
            else:
                self.cur.execute(query)
            return self.cur.fetchall()
        except Exception as e:
            print(f"An error occurred: {e}")
            return None

    def rollback(self):
        """Rollback the current transaction."""
        try:
            self.conn.rollback()
        except Exception as e:
            print(f"An error occurred during rollback: {e}")

    def close(self):
        """Close the cursor and connection."""
        if self.cur:
            self.cur.close()
        if self.conn:
            self.conn.close()
        print("Database connection closed.")
