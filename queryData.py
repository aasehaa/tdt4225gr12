# Query 6
from haversine import haversine, Unit
from DbConnector import DbConnector

class DBQuerySession:
    def __init__(self) -> None:
        """Constructor"""
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.db_connection.cursor(dictionary=True)
    
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

    def query_twelve(self):
        FIVE_MINUTES_DAYS = 5*60/86_400

        invalid_dict = dict()
        for i in range(1,182):
            invalid_dict[str(i).zfill(3)] = 0

        for id in invalid_dict.keys():
            query = """
            SELECT User.id, date_days
            FROM TrackPoint
                JOIN Activity ON activity_id = Activity.id
                JOIN User on user_id = User.id
            WHERE User.id = '%s'"""
            self.cursor.execute(query % id)
            TP = self.cursor.fetchall()

            for i in range(1, len(TP) + 1):
                if abs(TP[i-1]['date_days'] - TP[i]['date_days']) > FIVE_MINUTES_DAYS:
                    invalid_dict[id] += 1
        return invalid_dict
    
    def query_eleven(self):
        alt_gained = dict()
        for i in range(1,182):
            alt_gained[str(i).zfill(3)] = 0

        for id in alt_gained.keys():
            query = """
            SELECT User.id, date_days
            FROM TrackPoint
                JOIN Activity ON activity_id = Activity.id
                JOIN User on user_id = User.id
            WHERE User.id = '%s'"""
            self.cursor.execute(query % id)
            TP = self.cursor.fetchall()

            for i in range(1, len(TP) + 1):
                alt_difference = TP[i-1]['altitude'] - TP[i]['altitude']
                if alt_difference > 0:
                    if TP[i-1]['altitude'] != -777 and TP[i]['altitude'] != -777:
                        alt_gained[id] += alt_difference
            
            # Get the top 20 highest total altitudes
            top_users = sorted(alt_gained, key=alt_gained.get, reverse=True)[:20]
            print("Query 1\nUser\nAltitude gained")
            for usr in top_users:
                print(usr, alt_gained[usr])
        
        return alt_gained






def main():
    instance = None
    try:
        # Create instance and respective tables
        instance = DBQuerySession()
    except Exception as e:
        print("Unable to establish new session:\n", e, sep="")
    try:
        close_contacts = instance.query_six()
        print("Query 6:\nID\tNærkontakter")
        for key, value in close_contacts.items():
            print(key, value, sep="\t")
    except Exception as e:
        print("Unable to run query 6\n", e, sep="")

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