from tabulate import tabulate


def get_long_activity(self):
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
    print(len(number_of_users))
