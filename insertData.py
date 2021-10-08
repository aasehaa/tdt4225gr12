import os
# import pandas as pd
from datetime import datetime
from models import TrackPointObj
from DbConnector import DbConnector
from tabulate import tabulate
from typing import Tuple

try:
    from tqdm import tqdm
except:
    # TQDM is a progress bar that has to be pip-intalled.
    # It's used outside the entire os.walk-function
    # If user gets module error, we redefine the function to be indentity so tqm(os.walk(...)) can still run
    def tqdm(*args):
        return args


class DatabaseSession:
    def __init__(self) -> None:
        """Class constructor
        """
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor
        self.batchList = []  # To insert batches of data
        self.potential_matches = dict()

    def create_table(self, table_name: str) -> None:
        """Creates tables with appropriate schema given that the table name doesn't aldready exist
        :param table_name: 
        :type table_name: str
        """
        table_schema = {
            'User': """id VARCHAR(3) NOT NULL,
                        has_labels BOOLEAN,
                        PRIMARY KEY (id)""",
            'Activity': """id INT AUTO_INCREMENT NOT NULL,
                        user_id VARCHAR(3),
                        transportation_mode VARCHAR(10),
                        start_date_time DATETIME,
                        end_date_time DATETIME,
                        PRIMARY KEY (id),
                        FOREIGN KEY (user_id) REFERENCES User(id)
                        """,
            'TrackPoint': """id INT AUTO_INCREMENT NOT NULL,
                        activity_id INT,
                        lat DOUBLE,
                        lon DOUBLE,
                        altitude INT,
                        date_days DOUBLE,
                        date_time DATETIME,
                        PRIMARY KEY (id),
                        FOREIGN KEY (activity_id) REFERENCES Activity(id)"""
        }
        query = """CREATE TABLE IF NOT EXISTS %s ( %s )"""
        # This adds table_name to the %s variable and executes the query
        self.cursor.execute(query % (table_name, table_schema[table_name]))
        self.db_connection.commit()

    def insert_data(self, table_name: str, values: Tuple) -> None:
        """Method for inserting a single row/query to a given table

        :param table_name: Name of table in Database
        :type table_name: str
        :param values: Tuple of all values to be inserted  
        :type values: Tuple of varying size and data types
        """
        # Need to parse the columns in 'values` differently depending on what table we are inserting into
        if table_name == 'User':
            query = "INSERT INTO %s VALUES ('%s', %s)"
        elif table_name == 'Activity':
            query = "INSERT INTO %s (user_id, transportation_mode, start_date_time, end_date_time) VALUES ('%s', %s, '%s', '%s')"
        try:
            self.cursor.execute(query % (table_name, *values))
            self.db_connection.commit()
        except Exception as e:
            print("Unable to add values to table:", e)

    def insert_batch(self, table_name: str, values: Tuple, batchSize: int) -> None:
        """Specific method for adding batches of insertions simultaneously, only implemented for TrackPoint table

        :param table_name: Name of table (TrackPoint)
        :type table_name: str
        :param values: Values to be inserted
        :type values: Tuple
        :param batchSize: Number of additions to query at the same time
        :type batchSize: int
        """
        if len(self.batchList) < batchSize:
            self.batchList.append(values)
        else:
            for val in self.batchList:
                self.cursor.excecute("INSERT INTO %s VALUES (%s, %s, %s, %s,'%s','%s')" % (table_name, *val))
            self.db_connection.commit()
            self.batchList = [] # Resets temp storage of queries to be excecuted

    def drop_table(self, table_name):
        query = "DROP TABLE %s"
        self.cursor.execute(query % table_name)
        self.db_connection.commit()

    def clean_database(self):
        """Drops all tables in Database
        """
        self.cursor.execute("SHOW tables")
        all_tables = self.cursor.fetchall()[0]
        for table in all_tables:
            self.drop_table(self, table)

    def show_tables(self):
        self.cursor.execute("SHOW TABLES")
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))

    def apply_data(self, instance):
        """Main method scraping and fitting data from dataset, and inserting into the database

        :param instance: Instance-class containing database connection info
        :type instance: class
        """

        dataset_path = os.path.dirname(__file__) + "\\..\\dataset"

        with open(dataset_path + '/labeled_ids.txt', 'r') as fs:
            # Collect user IDs that has labeled activity
            labeled_IDs = fs.read().splitlines()

        for count, (root, dirs, files) in tqdm(enumerate(os.walk(dataset_path + '\\Data'))):
            if count == 0:
                # This part inserts rows in the User table. When count is 0, dirs will be a list of all user IDs ['001', ...].
                # For each of them, we check if they're labeled and assign the has_labels boolean accordingly.
                for id in dirs:
                    has_labels = id in labeled_IDs
                    instance.insert_data('User', (id, has_labels))
                    # Create a 2D list for each ID in a dictionary made for matching activities with their labels
                    self.potential_matches[id] = [[], [], []]

            if files != []:
                # This event is triggered if and only if
                # 1) We are in a User-folder with the one labels file
                # 2) We are in a Trajectory-folder with all plt-files
                # We implement case 1 first:
                for fn in files:

                    if fn[-3:] == 'txt':  # Case 1: Open the labels.txt file
                        with open(root + '/' + fn, 'r') as f:
                            all_rows = f.read().splitlines()
                            for row in all_rows[1:]:
                                start_time, end_time, mode = row.split('\t')

                                # Convert to DateTime:
                                start_time = start_time.replace("/", "-")
                                end_time = end_time.replace("/", "-")

                                # Add to dictionary
                                self.potential_matches[root[-3:]][0].append(start_time)
                                self.potential_matches[root[-3:]][1].append(end_time)
                                self.potential_matches[root[-3:]][2].append(mode)

                    elif fn[-3:] == 'plt':
                        with open(root + '/' + fn, 'r') as f:
                            activity = f.read().splitlines()[6:]
                            if len(activity) <= 2500:
                                transp_mode = "NULL"
                                # Save time as YYYY-MM-DD HH:MM:SS strings
                                activity_start = activity[0].split(',')[5] + ' ' + activity[0].split(',')[6]
                                activity_end = activity[-1].split(',')[5] + ' ' + activity[-1].split(',')[6]
                                
                                user = root[-14:-11]
                                if activity_start in self.potential_matches[user][0]:
                                    # This triggers when we find a match for the start time.
                                    # We also have to ensure that the corresponding end time also matches.
                                    ind = self.potential_matches[user][0].index(activity_start)
                                    if activity_end == self.potential_matches[user][1][ind]:
                                        transp_mode = "'" + self.potential_matches[user][2][ind] + "'"
                                current_user = user  # root is on the form Data\xxx\Trajectory so we extract xxx

                                instance.insert_data('Activity',
                                                     (current_user, transp_mode, activity_start, activity_end))
                                # instance.cursor.execute("SELECT LAST_INSERT_ID()")
                                # activity_ID = str(instance.cursor.fetchall()[0][0])
                                activity_ID = instance.cursor.lastrowid

                                track_points = []
                                for point in activity:
                                    lat, long, _, alt, timestamp, date, time = point.split(',')
                                    time_datetime = date + " " + time
                                    track_point = TrackPointObj.TrackPoint(activity_ID, lat, long, alt, timestamp, time_datetime)

                                    track_points.append(track_point)

                                track_points_values_string = ",".join(list(map(lambda tp: "(" + ",".join([str(tp.activity_id),
                                            str(tp.lat), str(tp.long), str(tp.altitude), str(tp.date_days), "'" +
                                                        str(tp.date_time) + "'", ]) + ")", track_points)))
                                query = """INSERT INTO TrackPoint(activity_id, lat, lon, altitude, date_days, date_time) 
                                            VALUES %s;""" % track_points_values_string

                                self.cursor.execute(query)
                                self.db_connection.commit()


def create_tables(self):
    self.create_table('User')
    self.create_table('Activity')
    self.create_table('TrackPoint')


def drop_tables(self):
    self.show_tables()
    self.drop_table('TrackPoint')
    self.drop_table('Activity')
    self.drop_table('User')


def main():
    """Main method for excecuting part 1 and part 2

    :param drop_data: Flag to determine if the generated tables should be dropped before ending program, defaults to False
    :type drop_data: bool, optional
    """
    print('Geolife data insertion')
    instance = None
    try:
        # Create instance and respective tables
        instance = DatabaseSession()
        print('Datasession start')

    except Exception as e:
        print("Unable to create database:", e)
    print('Applying data...')
    try:
        #instance.create_tables()
        #instance.apply_data(instance)
        instance.show_tables()
        #instance.drop_tables()
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        # Ensure connection is closed properly regardless of exceptions
        if instance:
            instance.connection.close_connection()


if __name__ == '__main__':
    main()
