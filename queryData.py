from haversine import haversine, Unit
from DbConnector import DbConnector

try:
    from tqdm import tqdm
except:
    def tqdm(*args):
        return args

class DBQuerySession:
    def __init__(self) -> None:
        """Constructor"""
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.db_connection.cursor(dictionary=True)

    def query_one(self):
        query1 = "SELECT COUNT(*) as 'number_of_users' FROM User;"
        query2 = "SELECT COUNT(*) as 'number_of_activities' FROM Activity;"
        query3 = "SELECT COUNT(*) as 'number_of_trackpoints' FROM TrackPoint;"

        self.cursor.execute(query1)  
        rows1 = self.cursor.fetchall()

        self.cursor.execute(query2) 
        rows2 = self.cursor.fetchall()

        self.cursor.execute(query3) 
        rows3 = self.cursor.fetchall()

        return rows1, rows2, rows3

    def query_two(self):
        query1 = """SELECT AVG(avg_activity) FROM 
        (SELECT COUNT(*) as avg_activity FROM Activity GROUP BY user_id) AS sub;"""
        query2 = """SELECT MIN(min_activity)  FROM 
        (SELECT COUNT(*) as min_activity FROM Activity GROUP BY user_id) AS sub;"""
        query3 = """SELECT MAX(max_activity) FROM 
        (SELECT COUNT(*) as max_activity FROM Activity GROUP BY user_id) AS sub;"""

        self.cursor.execute(query1)  
        rows1 = self.cursor.fetchall()

        self.cursor.execute(query2) 
        rows2 = self.cursor.fetchall()

        self.cursor.execute(query3) 
        rows3 = self.cursor.fetchall()

        return rows1, rows2, rows3

    def query_three(self):
        query = """SELECT COUNT(*) as 'activity_count', user_id
        FROM Activity GROUP BY user_id ORDER BY activity_count DESC LIMIT 10;"""

        self.cursor.execute(query)
        rows = self.cursor.fetchall()

        return rows

    def query_four(self):
        # Get acitivties start and end time
        query = "SELECT id, start_date_time, end_date_time FROM Activity"
        self.cursor.execute(query)
        rows = self.cursor.fetchall()

        # check if they have an end time the next day
        for row in rows:
            diff = row[2].day - row[1].day
            if diff != 1:
                rows.remove(row)

        # remove duplicate users
        number_of_users = []
        for row in rows:
            user = row[0]
            if user in number_of_users:
                continue
            else:
                number_of_users.append(user)
        print('\n')
        print('Number of users with activities > 1 day: \n', len(number_of_users))

    def query_five(self):
        query = """SELECT transportation_mode, COUNT(transportation_mode), 
        start_date_time, COUNT(start_date_time), end_date_time, COUNT(end_date_time) 
        FROM Activity
        GROUP BY transportation_mode, start_date_time, end_date_time
        HAVING COUNT(transportation_mode) > 1 AND COUNT(start_date_time) > 1 AND COUNT(end_date_time) > 1;"""

        self.cursor.execute(query)
        rows = self.cursor.fetchall()

        return rows
    
    def query_six(self):
        query = """
        SELECT User.id, lat, lon, altitude, date_days
        FROM TrackPoint
                join Activity on activity_id = Activity.id
                join User on user_id = User.id
        """
        self.cursor.execute(query)
        HOUNDED_METER_FEET = 328.08399 # 100m ~ 328 feet'
        SIXTY_SECONDS_DAYS = 60/86_400 # 86,400 seconds = 1 day
        contacts = dict()
        for i in range(1,182):
            contacts[str(i).zfill(3)] = [] # Initiate match-dictionary

        TrackPoint_row = self.cursor.fetchall()
        # [0: id], [1: lat], [2: lon], [3: altitude], [4: date_days],
        # Unless we have dictionary maybe

        for TP_A in TrackPoint_row:
            for TP_B in TrackPoint_row:
                if TP_A['id'] != TP_B['id']: # Exclude points from same user
                    if abs(TP_A['date_days'] - TP_B['date_days']) <= SIXTY_SECONDS_DAYS: #Exclude points larger than a minute away
                        coords_A = (TP_A['lat'], TP_A['lon'])
                        coords_B = (TP_B['lat'], TP_B['lon'])
                        distance = haversine(coords_A, coords_B, unit=Unit.METERS)
                        if distance < 100: # Exclude distances larger than 100m
                            if TP_A['altitude'] == -777: TP_A['altitude'] = 6 # Fix invalid altitudes
                            if TP_B['altitude'] == -777: TP_B['altitude'] = 6
                            if abs(TP_A['altitude'] - TP_B['altitude']) < HOUNDED_METER_FEET:
                                contacts[TP_A['id']].append(TP_B['id'])
                                contacts[TP_B['id']].append(TP_A['id'])
        return contacts

    def query_seven(self):
        query = """SELECT User.id, SUM(transportation_mode='taxi') AS numTaxi 
        FROM User JOIN Activity ON User.id = Activity.user_id
        GROUP BY User.id
        HAVING numTaxi < 1 or numTaxi IS NULL;"""
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        return rows

    def query_eight(self):
        query = """SELECT COUNT(DISTINCT user_id), transportation_mode 
        FROM Activity 
        WHERE transportation_mode IS NOT NULL 
        GROUP BY transportation_mode;"""

        self.cursor.execute(query)
        rows = self.cursor.fetchall()

        return rows

    def query_nine(self):
        # get all activities
        query = "SELECT * FROM Activity"
        self.cursor.execute(query)
        rows = self.cursor.fetchall()

        # group by year
        # assumption: only use start time, only find the month with most activities
        year_month_count = {}
        for activity in rows:
            year = activity[3].year
            month = activity[3].month
            if (year, month) in year_month_count:
                year_month_count[(year, month)] += 1
            else:
                year_month_count[(year, month)] = 1

        res_num = 0
        res_time = 0
        for time in year_month_count:
            if year_month_count[time] > res_num:
                res_num = year_month_count[time]
                res_time = time

        print('(year, month): ', res_time)
        print('number of activities: ', res_num)

        self.recorded_activities(res_time, rows)


    def recorded_activities(self, date, rows):
        # which user has the most activities this year and month?
        activities = rows
        user_activities = {}
        for activity in activities:

            year = activity[3].year
            month = activity[3].month
            if year != date[0]:
                # remove activities not in the right year
                activities.remove(activity)
                continue

            if month != date[1]:
                # remove activities not in the right month
                activities.remove(activity)
                continue

            user = activity[1]
            # counts activities per user
            if user in user_activities:
                user_activities[user] += 1
            else:
                user_activities[user] = 1

        best_user_activities = 0
        best_user_id = ''
        next_best_user_id = ''

        for user in user_activities:
            # loop over users to save best and next best per activity
            if best_user_activities < user_activities[user]:
                best_user_activities = user_activities[user]
                next_best_user_id = best_user_id
                best_user_id = user

        # how many recorded hours for the users
        hours_best_user = self.find_recorded_hours(best_user_id, rows)
        hours_next_best_user = self.find_recorded_hours(next_best_user_id, rows)

        print('\n')
        print('best user summary:')
        print('id: ', best_user_id, ' number of activities: ', best_user_activities, ' recorded hours: ',
              hours_best_user)
        print('\n')

        print('next best user summary:')
        print('id: ', next_best_user_id, ' recorded hours: ', hours_next_best_user)

    def find_recorded_hours(self, user, rows):
        activities = rows
        recorded_hours = 0
        # delete activities not related to user
        # assumption: not handling cases where activities are ending in another month
        # assumption: 1 month = 730 hours
        hours_per_month = 730
        hours_per_day = 24
        for activity in activities:
            if activity[1] != user:
                continue
            else:
                hours_per_activity = (activity[4].hour - activity[3].hour)
                days_per_activity = (activity[4].day - activity[3].day) * hours_per_day
                month_per_activity = (activity[4].month - activity[3].month) * hours_per_month
                recorded_hours += hours_per_activity + days_per_activity + month_per_activity

        return recorded_hours

    def query_ten(self):
        query = "SELECT lat, lon, start_date_time, end_date_time FROM TrackPoint JOIN Activity ON TrackPoint.activity_id = Activity.id WHERE Activity.user_id = '112' AND Activity.transportation_mode = 'walk';"
        self.cursor.execute(query)
        rows = self.cursor.fetchall()

        for row in rows:
            if row[2].year != 2008 and row[3].year != 2008:
                rows.remove(row)
        
        total_distance = 0
        for x in range(0, len(rows)-1):
            total_distance += haversine((rows[x][0], rows[x][1]),(rows[x+1][0], rows[x+1][1]))

        return total_distance
    
    def query_eleven(self):
        ONE_METER_FEET = 0.3048 # 1 foot ~0.3 feet
        alt_gained = dict()
        for i in range(1,182):
            alt_gained[str(i).zfill(3)] = 0

        for id in tqdm(alt_gained.keys()):
            query = """
            SELECT User.id, altitude
            FROM TrackPoint
                JOIN Activity ON activity_id = Activity.id
                JOIN User on user_id = User.id
            WHERE User.id = '%s'"""
            self.cursor.execute(query % id)
            TP = self.cursor.fetchall()

            for i in range(1, len(TP)):
                alt_difference = TP[i-1]['altitude'] - TP[i]['altitude']
                if alt_difference > 0:
                    if TP[i-1]['altitude'] != -777 and TP[i]['altitude'] != -777:
                        alt_gained[id] += alt_difference
            
        # Get the top 20 highest total altitude
        top_users = sorted(alt_gained, key=alt_gained.get, reverse=True)[:20]
        print("Query 11\nPlace\tUserID\tAltitude gained")
        for num, usr in enumerate(top_users):
            print(num+1, usr, alt_gained[usr]*ONE_METER_FEET, sep='\t')
        
        return alt_gained

    def query_twelve(self):
        invalid_dict = dict()
        for i in range(1,182):
            invalid_dict[str(i).zfill(3)] = [None, 0] # (curr_activity,  num)
        
        for id in tqdm(invalid_dict.keys()):
            query = """
            SELECT User.id, date_days, activity_id
            FROM TrackPoint
                JOIN Activity ON activity_id = Activity.id
                JOIN User on user_id = User.id
            WHERE User.id = '%s'"""
            self.cursor.execute(query % id)
            TP = self.cursor.fetchall()

            for i in range(1, len(TP)):  
                if TP[i-1]['activity_id'] == TP[i]['activity_id']: # Don't want to compare TPs from 2 different activities
                    if abs(TP[i-1]['date_days'] - TP[i]['date_days']) > FIVE_MINUTES_DAYS:
                        curr_activity = TP[i]['activity_id']
                        if invalid_dict[id][0] != curr_activity:
                            invalid_dict[id][0] = curr_activity #Ensures only one increment per activity
                            invalid_dict[id][1] += 1
        return invalid_dict





def main():
    instance = None
    try:
        # Create instance and respective tables
        instance = DBQuerySession()
    except Exception as e:
        print("Unable to establish new session:\n", e, sep="")
    # try:
    #     close_contacts = instance.query_six()
    #     print("Query 6:\nID\tN??rkontakter")
    #     for key, value in close_contacts.items():
    #         print(key, value, sep="\t")
    # except Exception as e:
    #     print("Unable to run query 6\n", e, sep="")

    # try:
    #     full_invalid = instance.query_twelve()
    #     print("Query12:\nUserID\t#Invalid activities")
    #     for key, value in full_invalid.items():
    #         if value[1] != 0:
    #             print(key, value[1], sep='\t')
    # except Exception as e:
    #     print("Unable to run query 12", e)
    
    try:
        count = instance.query_one()
        print("Query 1")
        for value in count:
            print(value, sep="\t")
    except Exception as e:
        print("Unable to run query 1\n", e, sep="")
    try:
        two = instance.query_two()
        print("Query 2")
        for value in two:
            print(value, sep="\t")
    except Exception as e:
        print("Unable to run query 2\n", e, sep="")
    try:
        high = instance.query_three()
        print("Query 3")
        for value in high:
            print(value, sep="\t")
    except Exception as e:
        print("Unable to run query 3\n", e, sep="")
    try:
        print("Query 3")
        instance.query_four()
    except Exception as e:
        print("Unable to run query 3\n", e, sep="")
    try:
        multiple = instance.query_five()
        print("Query 5")
        for value in multiple:
            print(value, sep="\t")
    except Exception as e:
        print("Unable to run query 5\n", e, sep="")
    try:
        close_contacts = instance.query_six()
        print("Query 6:\nID\tN??rkontakter")
        for key, value in close_contacts.items():
            print(key, value, sep="\t")
    except Exception as e:
        print("Unable to run query 6\n", e, sep="")
    try:
        no_taxi = instance.query_seven()
        print("Query 7")
        for value in no_taxi:
            print(value, sep="\t")
    except Exception as e:
        print("Unable to run query 7\n", e, sep="")
    try:
        t_mode = instance.query_eight()
        print("Query 8")
        for value in t_mode:
            print(value, sep="\t")
    except Exception as e:
        print("Unable to run query 8\n", e, sep="")
    try:
        print("Query 9")
        instance.query_nine()
    except Exception as e:
        print("Unable to run query 9\n", e, sep="")
    try:
        walked = instance.query_ten()
        print("Query 10")
        print(walked)
    except Exception as e:
        print("Unable to run query 10\n", e, sep="")
    try:
        alt = instance.query_eleven()
        print("Query 11")
        print(alt)
    except Exception as e:
        print("Unable to run query 11\n", e, sep="")
    try:
        full_invalid = instance.query_twelve()
        print("Query12:\nUserID\t#Invalid activities")
        for key, value in full_invalid.items():
            if value != 0:
                print(key, value, sep='\t')
    except:
        print("Unable to run query 12")
    finally:
        # Ensure connection is closed properly regardless of exceptions
        if instance:
            instance.connection.close_connection()

if __name__ == '__main__':
    main()