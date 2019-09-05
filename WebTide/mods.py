import commands
from datetime import datetime
import errno
import itertools
from math import sqrt, atan2, sin, cos
import numpy as np
import pandas as pd
import os
import re
import subprocess as sp
import sys

# calculate bearing of first point between two points
def calcBearing(lat1, lon1, lat2, lon2):
    theta = atan2(sin(lon2 - lon1) * cos(lat2), cos(lat1) * sin(lat2) - sin(lat1) * cos(lat2) *
                       cos(lon2 - lat1)) * (180 / np.pi)
    return theta

def webtideWorker(workerNum, df, wd, totalShips):
    print('Worker %d initialized' % workerNum)

    # constants
    ms2kts = 1.94384    # convert from m/s to kts
    PI = np.pi
    DEGREES = 180.0
    ship_id = str(df.iloc[0]['ship_id'])


    # webtide directories
    wdCfg = '/home/venasquezn/WebTide/WebTide/scripts/arctic'    # path of webtide config files
    # wdConfig = '/home/venasquezn/WebTide/WebTide/config_files/nwatl/'
    # wdConfig = '/home/venasquezn/WebTide/WebTide/config_files/arctic/'
    webInDir = wd + '/webIns'
    webOutDir = wd + '/webOuts'
    compDir = wd + '/compSpds'

    # create separate directories for webtide input and output files if they do not exist
    try:
        os.makedirs(webInDir)
        os.makedirs(webOutDir)
        os.makedirs(compDir)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise

    webExe = './tidecor'
    webCfg = './tidecor_arctic.cfg'
    webInFile = webInDir + '/webIn_' + ship_id + '.csv'
    webOutFile = webOutDir + '/webOut_' + ship_id + '.csv'
    compFile = compDir + '/comp_spds_' + ship_id + '.csv'
    bearingsFile = compDir + '/bearingTest_' + ship_id + '.csv'

    ##############################
    # I. Extract info from main df
    ##############################
    try:
        df = df.reset_index(drop=True)
        lon = df['long'].copy()
        lat = df['lat'].copy()
        date_time = df['date_time'].copy()
        spd = df['next_velocity'].copy()
        bearing = df['bearing'].copy()      # bearings column has NaN values - ignore for now



        # testing calcBearing
        new_bearing = []
        test = bearing.copy()
        for p in range(len(df) - 1):
            lat1 = df['lat'].iloc[p]
            lon1 = df['long'].iloc[p]
            lat2 = df['lat'].iloc[p + 1]
            lon2 = df['long'].iloc[p + 1]
            bearing1 = df['bearing'].iloc[p]
            nb = np.nan

            if bearing1 != bearing1 and df['rank'].iloc[p] != 99 and df['next_velocity'].iloc[p] != 0:
                nb = calcBearing(lat1, lon1, lat2, lon2)
            else:
                nb = bearing1
            new_bearing.append(nb)
        new_bearing.append(df['bearing'].iloc[-1])

        new_bearing = np.asarray(new_bearing)

        testDf = pd.DataFrame(columns=['lat', 'lon', 'bearing', 'new_bearing'])
        testDf['lat'] = lat
        testDf['lon'] = lon
        testDf['bearing'] = bearing
        testDf['new_bearing'] = np.asarray(new_bearing)

        testDf.to_csv(bearingsFile, index_label='id')
        # end testing



        # calculate u and v cpts of original speeds
        uCpt = spd * np.sin(new_bearing * (PI/DEGREES))
        vCpt = spd * np.cos(new_bearing * (PI/DEGREES))
        # uCpt and vCpt will get NaN values if bearings are NaN - change later

    except Exception as ex:
        print('Exception from part I: ship df info extraction, {0}'.format(ex))


    ##########################################
    # I. Create input file for webtide to read
    ##########################################
    try:
        webIn = pd.DataFrame(columns=['lon', 'lat', 'year', 'month', 'day', 'hour', 'minute'])

        # format date_time to meet webtide's input requirements
        date_time = [str(x) for x in df['date_time'].tolist()]

        date_time = [datetime.strptime(x, '%Y-%m-%d %H:%M:%S') if '.' not in x
                     else datetime.strptime(x, '%Y-%m-%d %H:%M:%S.%f') for x in date_time]
        year = [x.strftime('%Y') for x in date_time]
        month = [x.strftime('%m') for x in date_time]
        day = [x.strftime('%d') for x in date_time]
        hour = [x.strftime('%H') for x in date_time]
        minute = [x.strftime('%M') for x in date_time]

        dtCols = ['year', 'month', 'day', 'hour', 'minute']
        dtSers = [year, month, day, hour, minute]

        # assign lists-converted-to-series to webIn df
        for col, ser in itertools.izip(dtCols, dtSers):
            webIn[col] = np.asarray(pd.Series(ser))

        webIn['lon'] = np.asarray(lon)
        webIn['lat'] = np.asarray(lat)

        print('saving web input file for worker {0}'.format(workerNum))
        webIn.to_csv(webInFile, sep=' ', header=False, index=False)
    except Exception as ex:
        print('Exception from part II: creating Webtide input, {0}'.format(ex))


    #############################################################
    # II. Run WebTide in an external shell
    # # # generates output file of vectorized tidal currents
    #############################################################
    try:
        os.chdir(wdCfg)
        cmd = webExe + ' ' + webCfg + ' ' + webInFile + ' ' + webOutFile
        print('running webtide for worker {0}, ship {1}'.format(workerNum, ship_id))

        # write command to external shell
        exitCode = sp.call([webExe, webCfg, webInFile, webOutFile])

        if not(exitCode == 0):
            sys.exit('Problems running webtide for worker {0}, ship {1}'.format(workerNum,ship_id))
        print('webtide for worker {0} done'.format(workerNum))
    except Exception as ex:
        print('Exception from part III, {0}'.format(ex))


    #####################
    # Read webtide output
    #####################
    try:
        webOut = pd.read_csv(webOutFile, header = None)
        webOut.columns = ['webUcpt', 'webVcpt', 'lon', 'lat', 'year', 'month', 'day', 'hour', 'minute']

        webUcpt = webOut['webUcpt'].copy()
        webVcpt = webOut['webVcpt'].copy()
        webUcpt[webUcpt < -900] = 0
        webVcpt[webVcpt < -900] = 0
        webSpd = np.sqrt(webUcpt*webUcpt + webVcpt*webVcpt)

        # convert m/s to knots
        webUcpt = webUcpt * ms2kts
        webVcpt = webVcpt * ms2kts
        webSpd = webSpd * ms2kts

    except Exception as ex:
        print('Exception from part IV, {0}'.format(ex))


    #######################
    # calculate new speeds
    #######################
    try:
        newUcpt = uCpt - webUcpt
        newVcpt = vCpt - webVcpt
        newSpd = np.sqrt((newUcpt * newUcpt) + (newVcpt * newVcpt))
        # newUcpt, newVcpt and subsequently newSpd will get NaN values if uCpt and vCpt are NaN (from NaN bearings)


        # # NaN bearings for this ship
        # if ship_id == '100155000000000186':
        #     print(newSpd.head())


        compCols = ['spd', 'uCpt', 'vCpt', 'webSpd', 'webUcpt', 'webVcpt', 'newSpd', 'newUcpt', 'newVcpt']
        compSers = [spd, uCpt, vCpt, webSpd, webUcpt, webVcpt, newSpd, newUcpt, newVcpt]

        # compare speeds df
        compDf = pd.DataFrame(columns=compCols)

        for col, ser in itertools.izip(compCols, compSers):
            compDf[col] = ser

        # fill newSpd with original speed
        compDf['newSpd'].fillna(compDf['spd'], inplace=True)
        print('Max speed difference for ship {0}: {1}'.format(ship_id,
              abs(compDf['newSpd'] - compDf['spd']).max()))

        compDf.to_csv(compFile, index=False)

        df['new_velocity'] = newSpd

    except Exception as ex:
        print('Exception from part V, {0}'.format(ex))

    return workerNum, df
