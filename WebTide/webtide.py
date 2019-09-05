# runs webtide to generate tidal current speeds and directions at geospatial points
# uses these tidal speeds, original ship speeds and directions to calculate and correct speeds-relative-to-land to
# speeds-relative-to-water (actual ship speed)
#
# created by Naomi Venasquez
# last edited Nov 2017
from datetime import datetime
from mods import *
import multiprocessing
import pandas as pd

if __name__ == '__main__':
    wd = '/home/venasquezn/data/arctic/reprocessed_1122'
    inFile = wd + '/innav_arctic_ipr_1127.csv'
    outFile = wd + '/innav_arctic_web_ipr_1127.csv'

    processStart = datetime.now()
    cpuNum = 8

    print('initializing process at: {0}'.format(processStart))

    print('importing csv')

    df_main = pd.read_csv(inFile,
                          dtype={'id': str, 'ping_id': str, 'trip_id': str, 'ship_id': str, 'date_time': str,
                                 'rank': float, 'lat': float, 'long': float, 'rear_draught': float,
                                 'front_draught': float, 'midship_draught': float, 'dist_next_dest': float,
                                 'dist_next_waypt': float, 'calc_speed': float, 'adja_speed': float,
                                 'bearing': float, 'op_centre': str, 'object_id': str, 'object_type': str,
                                 'event_region': str, 'event_type': str, 'direction': str, 'mmsi': str,
                                 'ship_type': str, 'seaweb_ship_type': str, 'trip_start': str, 'trip_end': str,
                                 'trip_start_id': str, 'origin_id': str, 'origin_type': str,
                                 'origin_region': str, 'origin_country': str, 'trip_end_id': str,
                                 'dest_id': str, 'dest_type': str, 'dest_region': str, 'dest_country': str,
                                 'imo': str, 'extreme_breadth': float, 'max_power': float, 'country': str,
                                 'max_draught': float, 'length': float, 'deadweight': float, 'max_speed': float,
                                 'cargo_details': str, 'ship_name': str, 'year_built': str, 'breadth': float,
                                 'draught': float, 'displacement': float, 'gdt': float, 'ldt': float,
                                 'seg_ballast': str, 'slop_capacity': str, 'fuel_cap_1': float,
                                 'fuel_cap_2': float, 'eng_builder': str, 'eng_cyl': str, 'eng_design': str,
                                 'eng_model': str, 'eng_stroke': str, 'eng_type': str, 'eng_rpm': float,
                                 'total_kw_main_eng': float, 'eng_number': str, 'propeller_type': str,
                                 'aux_eng_builder': str, 'aux_eng_design': str, 'aux_eng_model': str,
                                 'aux_eng_stroke_type': str, 'aux_eng_total_kw': float, 'fuel_consumption': str,
                                 'engine_stroke_type': str, 'fuel_type_1': str, 'fuel_type_2': str,
                                 'vap_recovery': str, 'callsign': str, 'next_velocity': float,
                                 'change_in_time': float, 'date_time_old': str, 'activity_type': str,
                                 'activity_time': float, 'grid_index': float, 'ip': str, 'meit_region': str,
                                 'orig_meit_region': str, 'territory': str},
                          parse_dates=['date_time', 'date_time_old', 'trip_start', 'trip_end'], encoding='latin-1',
                          index_col='id')

    df_main.sort_values(by=['ship_id', 'trip_start', 'rank'], inplace=True)
    df_main.reset_index(drop=True, inplace=True)

    # save unique ship ids
    shipNames = df_main.ship_id.unique()
    totalShipAmt = len(shipNames)

    print('creating pools...')
    pool = multiprocessing.Pool(cpuNum, maxtasksperchild=5)

    tasks = []
    for sIter, shipName in enumerate(shipNames):
        # filter df only to ships in the chunk
        shipDf = df_main[df_main['ship_id'] == shipName]
        # add ship iterator, ship dataframe, and total number of ships to task
        tasks.append([sIter, shipDf, wd, totalShipAmt])

    # run tasks of each chunked data
    results = [pool.apply_async(webtideWorker, t) for t in tasks]

    first = True
    idxIter = 0
    for result in results:
        try:
            # get values from worker
            chunkNum, outDf = result.get()

            if len(outDf) > 0:
                # create new index for chunk
                outDf = outDf.assign(temp_idx=pd.Series(range(idxIter, idxIter + len(outDf))))
                outDf.set_index('temp_idx', inplace=True)

                outDf = outDf[
                        ['ship_id', 'trip_id', 'imo', 'mmsi', 'date_time', 'lat', 'long', 'grid_index',
                         'rank', 'ship_name', 'ship_type',
                         'next_velocity', 'activity_type', 'activity_time', 'adja_speed', 'aux_eng_builder',
                         'aux_eng_design', 'aux_eng_model', 'aux_eng_stroke_type', 'aux_eng_total_kw', 'bearing',
                         'breadth', 'calc_speed', 'callsign', 'cargo_details', 'country', 'date_time_old',
                         'deadweight', 'dest_country', 'dest_id', 'dest_region', 'dest_type', 'direction',
                         'displacement', 'dist_next_dest', 'dist_next_waypt', 'draught', 'eng_builder', 'eng_cyl',
                         'eng_design', 'eng_model', 'eng_number', 'eng_rpm', 'eng_stroke', 'eng_type',
                         'engine_stroke_type', 'extreme_breadth', 'front_draught', 'fuel_cap_1', 'fuel_cap_2',
                         'fuel_consumption', 'fuel_type_1', 'fuel_type_2', 'gdt', 'ldt', 'length', 'max_draught',
                         'max_power', 'max_speed', 'midship_draught', 'op_centre', 'origin_country', 'origin_id',
                         'origin_region', 'origin_type', 'propeller_type', 'rear_draught', 'seaweb_ship_type',
                         'seg_ballast', 'slop_capacity', 'total_kw_main_eng', 'trip_start', 'trip_end',
                         'trip_start_id', 'trip_end_id', 'vap_recovery', 'year_built', 'ip', 'meit_region',
                         'orig_meit_region', 'territory', 'new_velocity']]

                # # create df for checking results - only displays used columns and added columns
                # chDf = outDf[['ship_id', 'trip_id', 'lat', 'long', 'origin_country', 'dest_country', 'ip',
                #               'meit_region', 'orig_meit_region', 'territory']]
                #
                # chDf = chDf.assign(temp_idx=pd.Series(range(idxIter, idxIter + len(chDf))))
                # chDf.set_index('temp_idx', inplace=True)

                if first:
                    outDf.to_csv(outFile, mode='w', index_label='id', encoding='latin-1')
                    # chDf.to_csv(checkFile, mode='w', index_label='id', encoding='latin-1')
                    first = False
                else:
                    outDf.to_csv(outFile, mode='a', index_label='id', encoding='latin-1', header=False)
                    # chDf.to_csv(checkFile, mode='a', index_label='id', encoding='latin-1', header=False)
                print('ship chunk %d written to csv...' %chunkNum)

                # add dataframe length to iterator
                idxIter += len(outDf)

            else:
                print('empty dataframe...')

        except Exception as ex:
            print('Exception from main: %s' %str(ex))

    processTime = datetime.now() - processStart
    print('processing time %s' % processTime)

    print('total amount of ships in dataframe: %d' % totalShipAmt)
    pool.close()
    pool.join()
