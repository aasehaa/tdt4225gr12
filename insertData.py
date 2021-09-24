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
        if table_name == 'User': values = utilities.stringpad(values[0]) + ', ' + values[1]
        elif table_name == 'Activity':
            values[1], values[2] = utilities.stringpad(values[1]), utilities.stringpad(values[2])
            values = ','.join(values)
        else:
            values = ','.join(values)
        query = """INSERT INTO %s VALUES (%s)""" # TODO maybe write different queries for different tables...
        try:
            self.cursor.execute(query % (table_name, values))
            self.db_connection.commit()
        except Exception as e:
            print("Unable to add values " + values + " to table " + table_name + '\n' + e)

    def drop_table(self, table_name):
        query = "DROP TABLE %s"
        self.cursor.execute(query % table_name)

dataset_path = os.path.dirname(__file__) + "\\..\\dataset"
with open(dataset_path + '\\labeled_ids.txt', 'r') as fs:
    labeled_IDs = fs.read().splitlines()
instance = DatabaseSession()
instance.create_table('User')
instance.create_table('Activity')
instance.create_table('TrackPoints')

for count, root, dirs, files in enumerate(os.walk(dataset_path + '\\Data')):
    if count == 0:
        for id in dirs:
            has_labels = id in labeled_IDs
            instance.insert_data('User',(id, has_labels))

    if files != []:
        for fn in files:
            if fn[-3:] == 'plt':
                with open(fn, 'r') as f: #maybe we need entire directory path, unsure
                    if sum(1 for line in f) <= 2506:
                        activity = f.read().splitlines()[6:]
                        activity_start, activity_end = activity[0].split(',')[4], activity[-1].split(',')[4]
                        activity_start, activity_end = utilities.convert_timestamp(activity_start), utilities.convert_timestamp(activity_end)
                        current_user = root[5:8]

                        instance.insert_data('Activity', (current_user, "NULL", activity_start, activity_end))
                        instance.cursor.execute("SELECT LAST_INSERT_ID()")
                        activity_ID = int(instance.cursor.fetchall()[0][0])
                        for point in activity:
                            lat, long, _, alt, time, _, _ = point.split(',')
                            time_datetime = utilities.convert_timestamp(time)
                            instance.insert_data('Trackpoint', values=
                            (
                                activity_ID, lat, long, alt, time, time_datetime
                            )) #TODO make it so it inserts batches instead here!

            else: # Match labels to activity for some user
                with open(fn, 'r') as f:
                    pass
                    # Find start and end date
                    # Lookup 
# New pseudeocode, matching
# When adding activities, save start and end date somewhere in Python
# (After adding all activities and points)
# for ID in label_IDs:
    # for each label in txt file
        # if label.start == activity.start AND label.end == activity.end:
        #   change transportation mode in activity (SQL)
