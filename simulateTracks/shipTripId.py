# adds ship_id and trip_id to ferry tracks
import pandas as pd
from datetime import datetime

inFile = '/home/venasquezn/data/ferries/ferry_tracks_op.csv'
outFile = '/home/venasquezn/data/ferries/ferry_tracks_op_preCC.csv'

if __name__ == '__main__':

    processStart = datetime.now()

    # disables warning: 'A value is trying to be set on a copy of a slice from a DataFrame.'
    # enables value assignments to df[col].iloc[p]
    pd.options.mode.chained_assignment = None

    print('initializing process at: {0}'.format(processStart))
    print('importing main csv file: {0}'.format(inFile))
    print('        ... output file: {0}'.format(outFile))
    df = pd.read_csv(inFile,
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
                                     'activity_time': float, 'grid_index': float},
                              parse_dates=['date_time', 'date_time_old', 'trip_start', 'trip_end'], encoding='latin-1',
                              index_col='id')

    ship_id = 1000000
    trip_id = 1000000

    shipNames = df.ship_name.unique()

    first = True
    outDf = pd.DataFrame()
    for ship in shipNames:
        shipDf = df[df.ship_name == ship]
        shipDf['ship_id'] = ship_id
        # newRow = {}
        # newRow = newRow.fromkeys(shipDf.columns, 0)

        print('ship {0} initialized, size: {1}'.format(ship, len(shipDf)))

        for p in range(len(shipDf)):
            # for col in [str(x) for x in shipDf.columns]:
            #     # copy trip contents to newRow
            #     newRow[col] = str(shipDf[col].iloc[p])
            # newRow['trip_id'] = trip_id
            shipDf['trip_id'].iloc[p] = trip_id
            if shipDf['rank'].iloc[p] == 99:
                trip_id += 1
            # outDf = outDf.append(newRow, ignore_index=True)
            # outDf['ship_id'] = ship_id
        ship_id += 1

        if not first:
            shipDf = shipDf[shipDf['trip_id'] != 1000000]

        if first:
            shipDf.to_csv(outFile, mode='w', index_label='id', encoding='latin-1')
            first = False
        else:
            shipDf.to_csv(outFile, mode='a', index_label='id', encoding='latin-1', header=False)
        print('ship {0} written to csv...'.format(ship))

    processTime = datetime.now() - processStart
    print('processing time: {0}'.format(processTime))
