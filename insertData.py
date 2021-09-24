""" 
Insert data psudeocode
* Bruk os.walk
* For hver user:
    if user_dirname in labeled_ids.txt: has_labels = True
    sett inn bruker i DB
    for hver .plt
        activityID = mysql_connector.GetAutoIncrementThing()
        if antall lines > 2506: skip (husk å anta 2506 i rapporten)
        hent start [6] og slutt [-1] datetime
        Sett inn aktivitet i DB (med transportation_mode = '')
        if has_labels:
            sjekk at start in labels[start] og slutt in labels[slutt]
                hvis match: sett transportation_mode til labels[Mode]
        sett inn plt-data i TrackPoint DB:
        with open(filnavn) as f:
            skip til linje 6
            

        # Piazza-spørsmål: kan vi anta at data ikke er feil?   
## Batches of data instead?      
"""
import os
# import pandas as pd
from datetime import datetime
import utilities
from DbConnector import DbConnector
# from tabulate import tabulate

class DatabaseSession:
    def __init__(self) -> None:
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    def create_table(self, table_name):
        table_schema = {
            'User': """id STRING NOT NULL PRIMARY KEY,
                    has_labels BOOLEAN""",
            'Activity': """id INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
                        user_id STRING FOREIGN KEY REFERENCES User(id),
                        transportation_mode STRING,
                        start_date_time DATETIME,
                        end_date_time DATETIME
                        """,
            'TrackPoint': """id INT NOT NULL PRIMARY KEY,
                        activity_id INT FOREIGN KEY REFERENCES Activity(id),
                        lat DOUBLE,
                        lon DOUBLE,
                        altitude INT,
                        date_days DOUBLE,
                        date_time DATETIME"""
        }
        query = """CREATE TABLE IF NOT EXISTS %s ( %s )"""
        # This adds table_name to the %s variable and executes the query
        self.cursor.execute(query % table_name % table_schema[table_name])
        self.db_connection.commit()

    def insert_data(self, table_name, values):
        pass

    def drop_table(self, table_name):
        query = "DROP TABLE %s"
        self.cursor.execute(query % table_name)
