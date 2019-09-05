import fiona
from mods import *
from shapely.geometry import shape, LineString, mapping, Point, Polygon, MultiPolygon

meitRegions = '/home/venasquezn/data/meit_regions/meitregions_RegSplit/meitregions.shp'

coord_input = raw_input('Enter coords (lat,lon): ')
while (len(coord_input.split(',')) != 2):
    coord_input = raw_input('Incorrect input, try again:')

regionList = ([str(x) for x in range(23)] + ['-1-2', '-1-4', '0-5', '0-6', '0-7', '0-8', '0-9', '0-10'])[1:]
lat = float(coord_input.split(',')[0])
lon = float(coord_input.split(',')[1])

print('lat,lon: {0}, {1}'.format(lat,lon))

# save each polygon in shapefile to list
print('reading shapefiles...')
meitRegions_main = []
with fiona.open(meitRegions) as f:
    print('saving region polygons to list...')
    for poly in f:
        tempPoly = shape(poly['geometry'])
        region_id = str(poly['properties']['regID_new'])
        if str(region_id) in regionList:
            if tempPoly.type == 'Polygon':
                meitRegions_main.append((region_id, tempPoly))

# meit_region = findRegion(lon, lat, meitRegions_main)

for region in meitRegions_main:
        region_id = region[0]
        poly = region[1]

        # minx, miny, maxx, maxy = poly.bounds
        # # get bounding box coords of polygon
        # boundingBox = shapely.geometry.box(minx, miny, maxx, maxy)
        # if boundingBox.contains(Point(lon, lat)):
        if poly.contains(Point(lon, lat)):
                break
print('MEIT region: {0}'.format(region_id))
