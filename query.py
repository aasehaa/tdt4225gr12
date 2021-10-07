from haversine import haversine

def get_total_distance_walked(self):
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
