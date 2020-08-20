import sys
import os
import psycopg2
import psycopg2.extras
from pandas import pandas as pd, DataFrame


def populate_db_main():
    username = sys.argv[1]
    password = sys.argv[2]
    db_service = DBService(username, password)
    db_service.populate_db()


class DBService:
    """ 
    Responsible for populating database from RSPB Garden Birdwatch Results CSV
    Uses Pandas to read and interpret CSV, psycopg2 to write PostgreSQL database
    """

    def __init__(self, username, password):
        super().__init__()
        self.username = username
        self.password = password
        self.conn = None
        self.cursor = None

    def populate_db(self) -> None:
        """ Reads CSV and writes to county, bird and birdwatch_result tables """
        # Read RSPB CSV into DataFrame. Original page and .xlsx below
        # https://www.rspb.org.uk/get-involved/activities/birdwatch/results/
        # https://www.rspb.org.uk/globalassets/downloads/biggardenbirdwatch/2020/full-results2.xlsx
        rspb_results = os.getcwd() + "\\server\\scripts\\full-results.csv"
        df = pd.read_csv(rspb_results, header=0)
        # Setup database connection to db created in create_db.py
        self._setup_db_connection()
        # Write to county and bird tables, modifying DataFrame with database IDs
        df = self._write_counties(df)
        df = self._write_birds(df)
        # Finally, write to birdwatch_result table
        self._write_birdwatch(df)
        self.conn.close()

    def _setup_db_connection(self) -> None:
        """ Sets up database connection """
        self.conn = psycopg2.connect(
            dbname="birdwatch", user=self.username, host="", password=self.password
        )
        self.cursor = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    def _write_counties(self, df: DataFrame) -> DataFrame:
        """ Writes from CSV to county table """
        # Read unique county values from DataFrame
        # TODO: Handle formatting of Rhondda Cynon Taf
        # TODO: Work out what Lincolnshire (part of) means and how to handle this
        county_list = df.County.unique()
        county_values = []
        for county in county_list:
            rspb_office = df.query(f'County=="{county}"')["RSPB"].iloc[0]
            county_values.append(f"('{rspb_office}', '{county}')")
        # Construct and execute SQL to write unique counties to database
        counties_sql = f"INSERT INTO county (rspb_office, name) VALUES {', '.join(county_values)} RETURNING *;"
        self.cursor.execute(counties_sql)
        self.conn.commit()
        # Replace county names in DataFrame with IDs from database
        db_counties = [dict(county) for county in self.cursor.fetchall()]
        for county in db_counties:
            df = df.replace(county["name"], county["county_id"])
        return df

    def _write_birds(self, df: DataFrame) -> DataFrame:
        """ Writes from CSV to bird table """
        # Read unique bird values from DataFrame
        bird_list = df.Species.unique().tolist()
        bird_values = []
        for bird in bird_list:
            # Format bird names
            formatted_name = bird.replace("_", " ").title()
            bird_values.append(f"('{formatted_name}')")
            # Replace bird names in DataFrame so we can later link them through the db IDs
            df = df.replace(bird, formatted_name)
        # Construct and execute SQL to write unique birds to database
        birds_sql = (
            f"INSERT INTO bird (species) VALUES {', '.join(bird_values)} RETURNING *;"
        )
        self.cursor.execute(birds_sql)
        self.conn.commit()
        # Replace bird names in DataFrame with IDs from database
        db_birds = [dict(bird) for bird in self.cursor.fetchall()]
        for bird in db_birds:
            df = df.replace(bird["species"], bird["bird_id"])
        return df

    def _write_birdwatch(self, df: DataFrame) -> None:
        """ Writes from CSV to birdwatch_result table """
        # Construct and execute SQL to write birdwatch results to database
        birdwatch_list = []
        for _index, row in df.iterrows():
            result_values = f"({row['County']}, {row['Species']}, {row['% Gardens 2020']}, {row['% Gardens 2019']})"
            birdwatch_list.append(result_values)
        birdwatch_sql = f"INSERT INTO birdwatch_result (county_id, bird_id, percentage_2020, percentage_2019) VALUES {', '.join(birdwatch_list)};"
        self.cursor.execute(birdwatch_sql)
        self.conn.commit()


# Expects 2 parameters to be passed in:
#   1. Postgres username
#   2. Postgres password
if __name__ == "__main__":
    populate_db_main()
