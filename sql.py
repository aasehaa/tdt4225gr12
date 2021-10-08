from tabulate import tabulate


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

    recorded_activities(res_time, rows)


def recorded_activities(date, rows):
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
    hours_best_user = find_recorded_hours(best_user_id, rows)
    hours_next_best_user = find_recorded_hours(next_best_user_id, rows)

    print('\n')
    print('best user summary:')
    print('id: ', best_user_id, ' number of activities: ', best_user_activities, ' recorded hours: ', hours_best_user)
    print('\n')

    print('next best user summary:')
    print('id: ', next_best_user_id, ' recorded hours: ', hours_next_best_user)


def find_recorded_hours(user, rows):
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
            month_per_activity = (activity[4].month - activity[3].month)*hours_per_month
            recorded_hours += hours_per_activity + days_per_activity + month_per_activity

    return recorded_hours




