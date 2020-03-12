import psycopg2
import configparser

CONFIG = configparser.ConfigParser()
CONFIG.read("config.ini")


class PostgreSQL:

    def __init__(self):
        credentials = {
            "host": CONFIG["POSTGRESQL"]["DB_HOST"],
            "port": int(CONFIG["POSTGRESQL"]["DB_PORT"]),
            "database": CONFIG["POSTGRESQL"]["DB_NAME"],
            "user": CONFIG["POSTGRESQL"]["DB_USER"],
            "password": CONFIG["POSTGRESQL"]["DB_PASSWORD"]
        }
        self.debug = CONFIG["POSTGRESQL"].get("DEBUG") == "True"
        try:
            self._conn = psycopg2.connect(**credentials)
            self._cursor = self._conn.cursor()
            if self.debug:
                print("\nConnected to PostgreSQL\n")
        except psycopg2.Error as error:
            raise ValueError("Unable to connect to PostgreSQL database\n{error}".format(error=error))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def __del__(self):
        self.close()

    def close(self):
        self.connection.close()
        if self.debug:
            print("\nClosed PostgreSQL connection.\n")

    @property
    def connection(self):
        return self._conn

    @property
    def cursor(self):
        return self._cursor

    def commit(self):
        self.connection.commit()

    def query(self, sql, params=None):
        if self.debug:
            print(self.mogrify(sql, params))
        self.cursor.execute(sql, params or ())

    def mogrify(self, sql, params=None):
        return self.cursor.mogrify(sql, params or ())

    def fetchall(self):
        return self.cursor.fetchall()

    def fetchone(self):
        return self.cursor.fetchone()

    def insert(self, sql, params=None):
        if self.debug:
            print(self.mogrify(sql, params))
        self.cursor.execute(sql, params or ())
        self.commit()

    def insert_many(self, sql, params=None):
        params_str = ','.join((self.mogrify("%s", (x, ))).decode('utf-8') for x in params)
        if self.debug:
            print(self.mogrify(sql.format(params_str)))
        self.cursor.execute(sql.format(params_str))
        self.commit()

    def fetch_query_results(self, sql, params=None):
        if self.debug:
            print(self.mogrify(sql, params))
        self.cursor.execute(sql, params or ())
        while True:
            try:
                results = self.cursor.fetchmany(100)
                if not results:
                    break
                for result in results:
                    yield result
            except psycopg2.ProgrammingError:
                break
