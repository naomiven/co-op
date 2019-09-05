import fiona
from scipy import spatial
from math import radians, degrees, cos, sin, asin, atan2, sqrt, modf
import numpy as np
import pandas as pd
import shapely
from shapely.geometry import shape, LineString, mapping, Point, Polygon, MultiPolygon, box, LinearRing

# checks if string is empty (blank or repeated whitespaces)
def isempty(strval):
    if len(strval.split()) == 0:
        return True
    else:
        return False


# finds region for a set of lat/lons, with a variance of 1.1km from region boundaries
def findRegion(lon, lat, meitRegions_main):
    alloc = False
    for region in meitRegions_main:
        region_id = region[0]
        poly = region[1]

        minx, miny, maxx, maxy = poly.bounds
        # get bounding box coords of polygon to speed up elimination process
        boundingBox = shapely.geometry.box(minx, miny, maxx, maxy)
        if boundingBox.contains(Point(lon, lat)):
            if poly.contains(Point(lon, lat)):
                alloc = True
                break

    # if point is outside all meit regions, check if it's close to a meit region
    if alloc is False:
        for region in meitRegions_main:
            region_id = region[0]
            poly = region[1]
            # find coordinates on boundary closest to point
            pol_ext = LinearRing(poly.exterior.coords)
            d = pol_ext.project(Point(lon, lat))
            p = pol_ext.interpolate(d)
            # closest point coords on boundary
            cpc = list(p.coords)[0]
            dist = haversine(lon, lat, cpc[0], cpc[1])
            # if distance to boundary is < maxDist, assign current region_id
            if dist < 1.1:
                alloc = True
                break

    # if point is way outside meit region, assign temporary meit_region
    if alloc is False:
        print('ERROR: point is outside region or cannot allocate in region: {0}, {1}, {2}'.format(lat,lon,region_id))
        region_id = '-'
    return region_id


# finds distance between two sets of lat longs
def haversine(lon1, lat1, lon2, lat2):
    # convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    # haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat / 2.0) ** 2.0 + cos(lat1) * cos(lat2) * sin(dlon / 2.0) ** 2.0
    cee = 2.0 * asin(sqrt(a))
    km = 6367.0 * cee
    return km


# checks if trip crosses canadian waters - only for non-CAN orig and non-CAN dest
def crossCAN(trip, meitRegions_main):
    for u in range(len(trip)):
        tmpLat = trip['lat'].iloc[u]
        tmpLon = trip['long'].iloc[u]
        tmpRegion = findRegion(tmpLon, tmpLat, meitRegions_main)

        # if trip touches CAN waters, IP true
        if '-' not in tmpRegion:
            tmpIP = 'T'
            break
        else:
            tmpIP = np.nan

    return tmpIP


# main worker
def regAllocator(workerNum, df, meitRegions_main, totalShips):
    print('Worker %d initialized' % workerNum)

    CAN = 'CANADA'

    # create new dataframe to save added columns
    newShip = pd.DataFrame()

    trips = df.trip_id.unique()

    for t in trips:
        # save trip data
        newTrip = pd.DataFrame()
        trip = df[df.trip_id == t]

        if len(trip) > 0:
            newRow = {}
            newRow = newRow.fromkeys(trip.columns, 0)

            # add new columns, set default IP to false
            newRow['ip'] = 'F'
            newRow['meit_region'] = np.nan
            newRow['orig_meit_region'] = np.nan
            newRow['territory'] = np.nan
            ip = 'F'

            # extract info from first and last point of trip
            tripStart = trip.head(1)
            tripEnd = trip.tail(1)
            orig = tripStart['origin_country'].iloc[0]
            dest = tripEnd['dest_country'].iloc[0]
            origLat = tripStart['lat'].iloc[0]
            origLon = tripStart['long'].iloc[0]
            destLat = tripEnd['lat'].iloc[0]
            destLon = tripEnd['long'].iloc[0]

            # assign IPs
            # if orig or dest is Canadian, not an innocent passage
            if (CAN in orig) or (CAN in dest):
                newRow['ip'] = 'F'

            # if orig or dest or both empty
            elif (isempty(orig) is True) or (isempty(dest) is True):
                origRegion = findRegion(origLon, origLat, meitRegions_main)
                destRegion = findRegion(destLon, destLat, meitRegions_main)


                # if only orig is empty
                if (isempty(orig) is True) and (isempty(dest) is False):
                    if '-' not in origRegion:
                        newRow['ip'] = 'F'
                    else:
                        # check if trip touches CAN waters, either T or np.nan
                        newRow['ip'] = crossCAN(trip, meitRegions_main)

                # if only dest is empty
                elif (isempty(dest) is True) and (isempty(orig) is False):
                    if '-' not in destRegion:
                        newRow['ip'] = 'F'
                    else:
                        newRow['ip'] = crossCAN(trip, meitRegions_main)

                # if both are empty
                elif (isempty(orig) is True) and (isempty(dest) is True):
                    if ('-' not in origRegion) or ('-' not in destRegion):
                        newRow['ip'] = 'F'
                    else:
                        newRow['ip'] = crossCAN(trip, meitRegions_main)

            # if both orig and dest are not CAN and not empty, check if trip touches CAN waters
            else:
                newRow['ip'] = crossCAN(trip, meitRegions_main)


            # iterate through each trip
            for p in range(len(trip)):
                try:
                    for col in [str(x) for x in trip.columns]:
                        # copy trip contents to newRow
                        newRow[col] = str(trip[col].iloc[p])

                    lon = trip['long'].iloc[p]
                    lat = trip['lat'].iloc[p]

                    # find meit region for ping
                    region_id = findRegion(lon, lat, meitRegions_main)
                    orig_meit_region = region_id

                    if '-' not in orig_meit_region:
                        newRow['territory'] = CAN
                    else:
                        newRow['territory'] = 'USA'

                    if newRow['ip'] == 'F':
                        # if region is not Canadian, change region to official region (eg. 0-5 to 5)
                        if '-' in region_id:
                            region_id = region_id.split('-')[-1]
                    elif newRow['ip'] == 'T':
                        region_id = 'NA'
                    else:
                        region_id = np.nan

                    newRow['meit_region'] = region_id
                    newRow['orig_meit_region'] = orig_meit_region

                    newTrip = newTrip.append(newRow, ignore_index=True)

                except Exception as ex:
                    # print('Exception from mods: {0}'.format(ex))
                    template = 'Exception from mods: type {0} occured. Arguments: {1!r}'
                    message = template.format(type(ex).__name__,ex.args)
                    print(message)

        if len(newTrip) > 0:
            try:
                newShip = newShip.append(newTrip)
            except Exception as ex:
                print('Exception from mods, failed to append trip: {0}'.format(ex))

    print('size newShip: {0}'.format(len(newShip)))
    return workerNum, newShip
