import pandas as pd
from datetime import timedelta, datetime
import numpy as np
import itertools
import math

# returns True if num is NaN
def isnan(num):
    return num != num

# # checks if string is empty (blank or repeated whitespaces)
def isempty(strval):
    if len(strval.split()) == 0:
        return True
    else:
        return False

# removes duplicates in a list and preserves its order
def uniqueOrd(list):
    seen = set()
    seen_add = seen.add
    return [x for x in list if not (x in seen or seen_add(x))]

# main worker - creates tracks row by row
def genTracks(workerNum, df_op):
    print('Worker %d initialized' % workerNum)

    sep = ','
    newTrip = pd.DataFrame()
    newShip = pd.DataFrame()

    # iterate through each ship paths
    for p in range(len(df_op)):

        ######################################################
        # I. ASSIGN NAMES TO SHIP, POSITION, AND SCHEDULE INFO
        ######################################################

        try:
            # extract basic info from path
            ship_name = df_op['Vessel Name'].iloc[p]
            start_lat = df_op['start_lat'].iloc[p]
            start_lon = df_op['start_lon'].iloc[p]
            end_lat = df_op['end_lat'].iloc[p]
            end_lon = df_op['end_lon'].iloc[p]
            imo = df_op['IMO Number'].iloc[p]
            next_velocity = df_op['avg_speed_kn'].iloc[p]
            rev_time_int = float(df_op['rev_time_int'].iloc[p])
            wd = df_op['wd_op'].iloc[p]
            we = df_op['we_op'].iloc[p]
            wd_mid = df_op['wd_mid'].iloc[p]
            we_mid = df_op['we_mid'].iloc[p]
            exclude_days = df_op['exclude_days'].iloc[p]
            path = df_op['path'].iloc[p]

            # weekday start and end times
            wd_start, wd_end = df_op['wd_op'].iloc[p].split('-')[0], df_op['wd_op'].iloc[p].split('-')[1]
            wd_int = float(df_op['wd_int'].iloc[p])

            # if weekday mid sailing times are not empty, assign vars
            if not isnan(wd_mid):
                # if two mid sailing times
                if sep in wd_mid:
                    wd_mid1_start, wd_mid1_end = df_op['wd_mid'].iloc[p].split(',')[0].split('-')[0],\
                                                 df_op['wd_mid'].iloc[p].split(',')[0].split('-')[1]
                    wd_mid2_start, wd_mid2_end = df_op['wd_mid'].iloc[p].split(',')[1].split('-')[0],\
                                                 df_op['wd_mid'].iloc[p].split(',')[1].split('-')[1]
                    wd_mid1_int, wd_mid2_int = float(df_op['wd_mid_int'].iloc[p].split(',')[0]),\
                                               float(df_op['wd_mid_int'].iloc[p].split(',')[1])
                # else if only 1 mid sailing time
                else:
                    wd_mid1_start, wd_mid1_end = df_op['wd_mid'].iloc[p].split('-')[0],\
                                                 df_op['wd_mid'].iloc[p].split('-')[1]
                    wd_mid2_start = wd_mid2_end = np.nan
                    wd_mid1_int, wd_mid2_int = float(df_op['wd_mid_int'].iloc[p]), np.nan
            else:
                wd_mid1_start = wd_mid1_end = wd_mid2_start = wd_mid2_end = wd_mid1_int = wd_mid2_int = np.nan

            # weekend start and end times
            if not isnan(we):
                we_start, we_end = df_op['we_op'].iloc[p].split('-')[0], df_op['we_op'].iloc[p].split('-')[1]
                we_int = float(df_op['we_int'].iloc[p])
            else:
                we_start = we_end = we_int = np.nan

            # if weekend mid sailing times are not empty, assign vars
            if not isnan(we_mid):
                # if two mid sailing times
                if sep in we_mid:

                    we_mid1_start, we_mid1_end = df_op['we_mid'].iloc[p].split(',')[0].split('-')[0],\
                                                 df_op['we_mid'].iloc[p].split(',')[0].split('-')[1]
                    we_mid2_start, we_mid2_end = df_op['we_mid'].iloc[p].split(',')[1].split('-')[0],\
                                                 df_op['we_mid'].iloc[p].split(',')[1].split('-')[1]
                    we_mid1_int, we_mid2_int = float(df_op['we_mid_int'].iloc[p].split(',')[0]),\
                                               float(df_op['we_mid_int'].iloc[p].split(',')[1])
                # else if only 1 mid sailing time
                else:
                    we_mid1_start, we_mid1_end = df_op['we_mid'].iloc[p].split('-')[0],\
                                                 df_op['we_mid'].iloc[p].split('-')[1]
                    we_mid2_start = we_mid2_end = np.nan
                    we_mid1_int, we_mid2_int = float(df_op['we_mid_int'].iloc[p]), np.nan
            else:
                we_mid1_start = we_mid1_end = we_mid2_start = we_mid2_end = we_mid1_int = we_mid2_int = np.nan

            # for day exlusions:
            excluded_days = []

            if not isnan(exclude_days):
                excluded_days = exclude_days.split()

            print('Ship Name: {0}, Path: {1}'.format(ship_name, path))

        except Exception as ex:
            print('Exception from mods -> I. Name Assignment: {0}'.format(ex))


        #########################################################
        # II. CONVERT SAILING TIMES (STRINGS) TO DATETIME OBJECTS
        #########################################################

        try:
            # create list of timestamps for a day
            wd_start = datetime.strptime(wd_start, '%H:%M')
            wd_end = datetime.strptime(wd_end, '%H:%M')

            if not isnan(we):
                we_start = datetime.strptime(we_start, '%H:%M')
                we_end = datetime.strptime(we_end, '%H:%M')

            # if mid sailing times are not empty, convert
            if not isnan(wd_mid):
                wd_mid1_start = datetime.strptime(wd_mid1_start, '%H:%M')
                wd_mid1_end = datetime.strptime(wd_mid1_end, '%H:%M')
                if not isnan(wd_mid2_start):
                    wd_mid2_start = datetime.strptime(wd_mid2_start, '%H:%M')
                    wd_mid2_end = datetime.strptime(wd_mid2_end, '%H:%M')

            if not isnan(we_mid):
                we_mid1_start = datetime.strptime(we_mid1_start, '%H:%M')
                we_mid1_end = datetime.strptime(we_mid1_end, '%H:%M')
                if not isnan(we_mid2_start):
                    we_mid2_start = datetime.strptime(we_mid2_start, '%H:%M')
                    we_mid2_end = datetime.strptime(we_mid2_end, '%H:%M')


            curr_time = wd_start
            wd_timestamps = []
            we_timestamps = []
            wd_all = uniqueOrd([wd_start, wd_mid1_start, wd_mid1_end, wd_mid2_start, wd_mid2_end, wd_end])
            we_all = uniqueOrd([we_start, we_mid1_start, we_mid1_end, we_mid2_start, we_mid2_end, we_end])
            # removes timestamps with NaN values in wd_all and we_all
            wd_time_pts = [x for x in wd_all if not isnan(x)]
            we_time_pts = [x for x in we_all if not isnan(x)]
            time_pts = [wd_time_pts, we_time_pts]

            print('... {0} {1} wd time checkpoints: {2}'.format(ship_name, path, [x.strftime('%H:%M') for x in wd_time_pts]))
            print('... {0} {1} we time checkpoints: {2}'.format(ship_name, path, [x.strftime('%H:%M') for x in we_time_pts]))

        except Exception as ex:
            print('Exception from mods -> II. Conversion to Datetime Object: {0}'.format(ex))


        ##############################################################
        # III. CREATE LISTS OF SAILING TIMES FOR WEEKDAYS AND WEEKENDS
        ##############################################################

        try:
            for d in time_pts:
                # iterate through sailing times and append to weekday or weekend list
                if len(d) > 0:
                    for i, t in enumerate(d):
                        curr_time = t
                        # if weekday, add current time to list, and set time intervals b/w sailing times
                        if d is wd_time_pts:
                            wd_timestamps.append(t)
                            h = wd_int
                            if t is wd_mid1_start:
                                h = wd_mid1_int
                            elif t is wd_mid2_start:
                                h = wd_mid2_int
                        # if weekend, add current time to list, and set time intervals b/w sailing times
                        else:
                            we_timestamps.append(t)
                            h = we_int
                            if t is we_mid1_start:
                                h = we_mid1_int
                            elif t is we_mid2_start:
                                h = we_mid2_int

                        # t2 is next sailing time
                        if i != (len(d)-1):
                            t2 = d[i+1]
                        else:
                            t2 = t

                        if curr_time > t2:
                            t2 = t2 + timedelta(hours=24)

                        while curr_time < t2 - timedelta(hours=h):
                            next_time = curr_time + timedelta(hours=h)
                            # if next_time is at least 5 minutes before the next sailing time, append to timestamps
                            if next_time + timedelta(minutes=5) < t2:
                                if d is wd_time_pts:
                                    # wd_timestamps.append(next_time.strftime('%H:%M'))
                                    wd_timestamps.append(next_time)
                                else:
                                    # we_timestamps.append(next_time.strftime('%H:%M'))
                                    we_timestamps.append(next_time)
                            curr_time = next_time
                else:
                    print('{0} is empty'.format(d))

            # sort datetimes in order
            wd_timestamps = sorted([x.time() for x in wd_timestamps])
            we_timestamps = sorted([x.time() for x in we_timestamps])

            print('... {0} {1} weekday timestamps: {2}'.format(ship_name, path,
                                                           [x.strftime('%H:%M') for x in wd_timestamps]))
            print('... {0} {1} weekend timestamps: {2}'.format(ship_name, path,
                                                           [x.strftime('%H:%M') for x in we_timestamps]))
        except Exception as ex:
            print('Exception from mods -> III. Sailing Times: {0}'.format(ex))


        ####################################################
        # IV. ITERATE THROUGH DATES AND ADD ROWS TO NEW TRIP
        ####################################################

        try:
            # create new row for ping info
            newRow = {}

            object_type = 'Wharf'

            # basics
            year = '2015'
            weekdays = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri']
            weekends = ['Sat', 'Sun']
            start_month = int(df_op['months_active'].iloc[p].split(',')[0])
            end_month = int(df_op['months_active'].iloc[p].split(',')[1])
            first_day = 1

            if end_month in [1,3,5,7,8,10,12]:
                last_day = 31
            elif end_month == 2:
                last_day = 28
            else:
                last_day = 30

            # convert start date and end date to datetime objects
            start_date = datetime.strptime('-'.join(str(x) for x in [year, start_month, first_day]), '%Y-%m-%d')
            end_date = datetime.strptime('-'.join(str(x) for x in [year, end_month, last_day]), '%Y-%m-%d')

            print('... {0} start date: {1} {2}'.format(ship_name, start_date, end_date))

            curr_date = start_date

            print('--> creating tracks for worker {0}, ship: {1}, path: {2}'.format(workerNum, ship_name, path))

            # iterate through start to end date
            while curr_date < end_date + timedelta(days=1):
                day = curr_date.strftime('%a')

                if day not in excluded_days:
                    # if day is a weekday, assign weekday hours, else assign weekend hours
                    if day in weekdays:
                        timestamps = wd_timestamps
                    else:
                        timestamps = we_timestamps

                    # iteration for 1 day - add rows for each sailing time t
                    for t in timestamps:
                        # curr_time = datetime.strptime(t, '%H:%M')
                        curr_time = t       # datetime.time object
                        # combine date with hours
                        date_time = datetime.combine(datetime.date(curr_date), curr_time)

                        cols_str = ['ship_name', 'lat', 'lon', 'rank', 'date_time', 'day', 'imo', 'next_velocity',
                                    'object_type', 'rev', 'path']
                        cols_rank1 = [ship_name, start_lat, start_lon, 1, date_time, day, imo, next_velocity,
                                      object_type, 0, path]
                        cols_rank99 = [ship_name, end_lat, end_lon, 99, date_time, day, imo, next_velocity,
                                       object_type, 0, path]
                        # columns for reverse trips
                        cols_rank1_rev = [ship_name, end_lat, end_lon, 1, date_time + timedelta(hours=rev_time_int),
                                          day, imo, next_velocity, object_type, 1, path]
                        cols_rank99_rev = [ship_name, start_lat, start_lon, 99, date_time + timedelta(hours=rev_time_int),
                                           day, imo, next_velocity, object_type, 1, path]

                        cols_all = [cols_rank1, cols_rank99, cols_rank1_rev, cols_rank99_rev]

                        for cols in cols_all:
                            for i, j in itertools.izip(cols_str, cols):
                                newRow[i] = j
                            newTrip = newTrip.append(newRow, ignore_index=True)

                # go to next day
                curr_date = curr_date + timedelta(days=1)

        except Exception as ex:
            print('Exception from mods -> IV. Adding Rows: {0}'.format(ex))

    newTrip.index.name = 'id'
    # newTrip.sort_values(by=['path'], inplace=True)
    newTrip.loc[newTrip['rank'] == 99, 'date_time'] = np.nan
    newTrip.loc[newTrip['rank'] == 99, 'next_velocity'] = np.nan

    if len(newTrip) > 0:
            try:
                newShip = newShip.append(newTrip)
            except Exception as ex:
                print('Exception from mods, failed to append trip: {0}'.format(ex))

    print('size newShip: {0}'.format(len(newShip)))
    return workerNum, newShip
