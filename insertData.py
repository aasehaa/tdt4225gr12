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
try:
    from tqdm import tqdm
except:
    pass #noqa

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
        # Need to parse the columns in 'values` differently depending on what table we are inserting into
        if table_name == 'User':
            values = utilities.stringpad(values[0]) + ', ' + values[1]
        elif table_name == 'Activity':
            # Second and third column has to be padded with single quotes.
            values[1], values[2] = utilities.stringpad(values[1]), utilities.stringpad(values[2])
            values = ','.join(values)
        else:
            values = ','.join(values)
        query = """INSERT INTO %s VALUES (%s)"""
        try:
            self.cursor.execute(query % (table_name, values))
            self.db_connection.commit()
        except Exception as e:
            print("Unable to add values " + values + " to table " + table_name + '\n' + e)

    def drop_table(self, table_name):
        query = "DROP TABLE %s"
        self.cursor.execute(query % table_name)
    def clean_database(self):
        pass
        # Drop all tables

dataset_path = os.path.dirname(__file__) + "\\..\\dataset"
with open(dataset_path + '\\labeled_ids.txt', 'r') as fs:
    # Collect user IDs that has labeled activity
    labeled_IDs = fs.read().splitlines()
instance = DatabaseSession()
instance.create_table('User')
instance.create_table('Activity')
instance.create_table('TrackPoints')

for count, root, dirs, files in enumerate(os.walk(dataset_path + '\\Data')):
    potential_matches = dict() # TODO is this very large and inefficient?
    if count == 0:
        # This part inserts rows in the User table. When count is 0, dirs will be a list of all user IDs ['001', ...].
        # For each of them, we cehck if they're labeled and assign the has_labels boolean accordingly.
        for id in dirs:
            has_labels = id in labeled_IDs
            instance.insert_data('User',(id, has_labels))
            potential_matches[id] = [[], [], []]


    if files != []:
        # This event is triggered if and only if
        # 1) We are in a User-folder with the one labels file
        # 2) We are in a Trajectory-folder with all plt-files
        # We implement case 1 first:
        for fn in files:

            if fn[-3] == 'txt':
                with open(fn, 'r') as f:
                    all_rows = f.read().splitlines()
                    for row in all_rows[1:]:
                        start_time, end_time, mode = row.split('\t')
                        # Convert times to DateTime form
                        # potential_matches[root[5:8]][0].append(start_time) etc etc

            else:
                with open(fn, 'r') as f: # TODO maybe we need entire directory path, unsure
                    if sum(1 for line in f) <= 2506: # Skip all files with more than 2506 lines.
                        transp_mode = "NULL"
                        activity = f.read().splitlines()[6:]
                        activity_start, activity_end = activity[0].split(',')[4], activity[-1].split(',')[4]
                        activity_start, activity_end = utilities.convert_timestamp(activity_start), utilities.convert_timestamp(activity_end)
                        if activity_start in potential_matches[root[5:8]][0]:
                            pass
                            # Get index in potential_matches
                            # if activity_end == potential_matches[root[5:8]][1][ind]:
                                # transp_mode = potential_matches[root[5:8]][2][ind]
                        current_user = root[5:8] # root is on the form Data\xxx\Trajectory so we extract xxx

                        instance.insert_data('Activity', (current_user, transp_mode, activity_start, activity_end))
                        instance.cursor.execute("SELECT LAST_INSERT_ID()")
                        activity_ID = int(instance.cursor.fetchall()[0][0])
                        for point in activity:
                            lat, long, _, alt, time, _, _ = point.split(',')
                            time_datetime = utilities.convert_timestamp(time)
                            instance.insert_data('Trackpoint', values=
                            (
                                activity_ID, lat, long, alt, time, time_datetime
                            )) #TODO make it so it inserts batches instead here!
