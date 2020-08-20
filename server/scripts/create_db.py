import sys
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

def create_db_main() -> None:
    """ Created birdwatch db and tables, using supplied PostgreSQL username/password """
    username = sys.argv[1]
    password = sys.argv[2]
    db_service = DBCreateService(username, password)
    db_service.create_db()
    db_service.create_tables()

class DBCreateService:
    """ Class responsible for creating db and tables required """

    def __init__(self, username, password):
        super().__init__()
        self.username = username
        self.password = password

    def create_db(self) -> None:
        """ Create database named 'birdwatch' """
        conn = psycopg2.connect(
            dbname='postgres', user=self.username, host='', password=self.password)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        cursor.execute("CREATE DATABASE birdwatch")
        conn.close()

    def create_tables(self) -> None:
        """ Creates 'county', 'bird', and 'birdwatch_result' tables """
        sql = self.get_create_tables_sql()
        conn = psycopg2.connect(
            dbname='birdwatch', user=self.username, host='', password=self.password)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        cursor.execute(sql)
        conn.close()

    def get_create_tables_sql(self) -> str:
        """ Returns SQL for table creation """
        sql = """
            DROP TABLE IF EXISTS birdwatch_result;
            DROP TABLE IF EXISTS county;
            DROP TABLE IF EXISTS bird;
            CREATE TABLE county (
                county_id SERIAL PRIMARY KEY,
                name VARCHAR(100) UNIQUE,
                rspb_office VARCHAR(20)
            );
            CREATE TABLE bird (
                bird_id SERIAL PRIMARY KEY,
                species VARCHAR(100) UNIQUE
            );
            CREATE TABLE birdwatch_result(
                result_id SERIAL PRIMARY KEY,
                county_id SMALLINT,
                bird_id SMALLINT,
                percentage_2020 NUMERIC,
                percentage_2019 NUMERIC,
                CONSTRAINT fk_county_id FOREIGN KEY(county_id) REFERENCES county(county_id),
                CONSTRAINT fk_bird_id FOREIGN KEY(bird_id) REFERENCES bird(bird_id)
            );
        """
        return sql

# Expects 2 parameters to be passed in:
#   1. Postgres username
#   2. Postgres password
if __name__ == '__main__':
    create_db_main()