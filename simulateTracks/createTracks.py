# creates tracks for ferries in Operator_Info
# output file: can be processed for innav scripts, but skip DVAT
#
# Created by Naomi Venasquez
# Last edited November 2017

import multiprocessing
from mods import *

# CONSTANTS
sep = ','
start_trip_id = 1000000

processStart = datetime.now()

if __name__ == '__main__':
    test_file ='/home/venasquezn/data/ferries/ferry_test.csv'
    outFile = '/home/venasquezn/data/ferries/ferry_tracks_op.csv'

    processStart = datetime.now()
    cpuNum = 8

    df_op = pd.read_csv('/home/venasquezn/data/ferries/ferry_test.csv')
    df_op.replace({'#N/A':np.nan}, regex=True)
    # df_op.fillna('', inplace=True)

    print('creating pools...')
    pool = multiprocessing.Pool(cpuNum, maxtasksperchild=5)

    # save unique ship names
    shipNames = df_op['Vessel Name'].unique()

    tasks = []
    for sIter, shipName in enumerate(shipNames):
        # filter df only to ships in the chunk
        shipDf = df_op[df_op['Vessel Name'] == shipName]
        # add ship iterator, ship dataframe, and total number of ships to task
        tasks.append([sIter, shipDf])


    # run tasks of each chunked data
    results = [pool.apply_async(genTracks, t) for t in tasks]

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

                all_cols = ['ship_id', 'trip_id', 'imo', 'mmsi', 'date_time', 'lat', 'long', 'grid_index', 'rank',
                    'ship_name', 'ship_type', 'next_velocity', 'activity_type', 'activity_time', 'adja_speed',
                    'aux_eng_builder', 'aux_eng_design', 'aux_eng_model', 'aux_eng_stroke_type',
                    'aux_eng_total_kw', 'bearing', 'breadth', 'calc_speed', 'callsign', 'cargo_details',
                    'country', 'date_time_old', 'deadweight', 'dest_country', 'dest_id', 'dest_region',
                    'dest_type', 'direction', 'displacement', 'dist_next_dest', 'dist_next_waypt', 'draught',
                    'eng_builder', 'eng_cyl', 'eng_design', 'eng_model', 'eng_number', 'eng_rpm', 'eng_stroke',
                    'eng_type', 'engine_stroke_type', 'extreme_breadth', 'front_draught', 'fuel_cap_1',
                    'fuel_cap_2', 'fuel_consumption', 'fuel_type_1', 'fuel_type_2', 'gdt', 'ldt', 'length',
                    'max_draught', 'max_power', 'max_speed', 'midship_draught', 'op_centre', 'origin_country',
                    'origin_id', 'origin_region', 'origin_type', 'propeller_type', 'rear_draught',
                    'seaweb_ship_type', 'seg_ballast', 'slop_capacity', 'total_kw_main_eng', 'trip_start',
                    'trip_end', 'trip_start_id', 'trip_end_id', 'vap_recovery', 'year_built']

                missing_cols = list(set(all_cols) - set(outDf.columns.tolist()))

                for col in missing_cols:
                    outDf[col] = np.nan

                # outDf = outDf[all_cols]

                outDf.sort_values(by=['ship_name'])
                # outDf.loc[outDf['rank'] == 99, 'date_time'] = np.nan
                # outDf.loc[outDf['rank'] == 99, 'next_velocity'] = np.nan
                outDf.reset_index(drop=True)    # drop=True to prevent being saved as a column

                if first:
                    outDf.to_csv(outFile, mode='w', index_label='id', encoding='latin-1')
                    first = False
                else:
                    outDf.to_csv(outFile, mode='a', index_label='id', encoding='latin-1', header=False)
                print('ship chunk %d written to csv...' %chunkNum)

                # add dataframe length to iterator
                idxIter += len(outDf)

            else:
                print('empty dataframe...')

        except Exception as ex:
            print('Exception from main: %s' %str(ex))

    processTime = datetime.now() - processStart
    print('processing time %s' % processTime)

    # print('total amount of ships in dataframe: %d' % totalShipAmt)
    pool.close()
    pool.join()

