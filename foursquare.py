from __future__ import print_function
from operator import add

from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
import pyspark.sql.functions as pyspark_f
from pyspark.sql.types import LongType, ArrayType
import matplotlib.pyplot as plt
import kdtree
import time
from datetime import datetime, timedelta
from math import radians, cos, sin, asin, atan2, sqrt, degrees, log


def haversine_path(pos_seq):
    _lon, _lat = None, None
    path = 0
    for lon, lat in pos_seq:
        if _lon is None or _lat is None:
            _lon, _lat = lon, lat
            continue
        path += haversine(_lon, _lat, lon, lat)
    return path


def haversine_path_dict(items):
    _lon, _lat = None, None
    path = 0
    for item in items:
        lon, lat = item['pos']
        if _lon is None or _lat is None:
            _lon, _lat = lon, lat
            continue
        path += haversine(_lon, _lat, lon, lat)
    return path


def haversine(lon1, lat1, lon2, lat2, r=6371):
    """
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees)
    :param lon1:
    :param lat1:
    :param lon2:
    :param lat2:
    :param r:
    """
    # r = radius of earth in kilometers. Use 3956 for miles
    # convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, map(float, [lon1, lat1, lon2, lat2]))

    # haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * asin(sqrt(a))
    return c * r


def timestamp(stamp, tz):
    date, time = stamp.split(' ')
    year, month, day = map(int, date.split('-'))
    hour, minute, sec = map(int, time.split(':'))
    return datetime(year=year, month=month, day=day, hour=hour, minute=minute, second=sec) + timedelta(minutes=int(tz))


def unique_column_count(sc_file, col_n=0):
    """
    Count unique items in a Tabular Separated Values file by column.
    :param sc_file: SparkContext File, preferably TSV
    :param col_n: Column to count
    :return:

    """
    return sc_file.map(lambda row: row[col_n]).distinct().count()


def cartesian_to_latlong(x, y, z, r=6371.0):
    """
    Convert 3-d cartesian point to latitude, longitude point
    :param x:
    :param y:
    :param z:
    :param r:
    :return:
    """
    lat = asin(z / r)
    lon = atan2(y, x)
    return degrees(lat), degrees(lon)


def cartesian_haversine(node, point):
    """
    Haversine function on 3-d cartesian point
    :param node:
    :param point:
    :return:
    """
    x, y, z = node
    x1, y1, z1 = point
    lat, lon = cartesian_to_latlong(x, y, z)
    lat1, lon1 = cartesian_to_latlong(x1, y1, z1)
    return haversine(lat1, lon, lat1, lon1)


def latlong_to_cartesian(lat, lon, r=6371.0):
    """
    Convert latitude, longitude point to 3-d cartesian
    :param lat:
    :param lon:
    :param r:
    :return:
    """
    lat = radians(lat)
    lon = radians(lon)

    x = r*cos(lon)*sin(lat)
    y = r*sin(lon)*sin(lat)
    z = r*cos(lat)
    return x, y, z


def kdtree_query(tree, lat, lon):
    """
    Query the given 3-d tree with latitude and longitude, using conversion from lat, lon -> 3-d cartesian
    :param tree:
    :param lat:
    :param lon:
    :return:
    """
    coords = latlong_to_cartesian(float(lat), float(lon))
    result = tuple(tree.search_nn(coords))[0].data
    return result


def task_3(foursqr, cities):
    a = time.time()
    # Create a dictionary where the key is a
    # tuple 3-d cartesian coordinates, and the value is a row
    cartesian_coords_cities_dict = dict(cities
                                        .map(lambda row: (
                                        tuple(latlong_to_cartesian(row.lat, row.lon)),
                                        row))
                                        .collect())
    # Create a k-d tree (3-d in this case) for searching
    tree = kdtree.create(cartesian_coords_cities_dict.keys())

    # For each row in foursqr
    # Query the k-d tree for a nearest neighbour
    # return neighbour and row
    cart_user_city = foursqr.map(lambda row:
                                   (
                                       cartesian_coords_cities_dict[
                                           kdtree_query(tree, row.lat, row.lon)
                                       ]
                                       , row
                                   )
                                   )

    print("KDTree: ", time.time() - a)

    return cart_user_city


def task_4(foursqr, cities):
    print('a) Unique users (distinct user_ids): ', unique_column_count(foursqr, 1))
    print('b) Total checkins (distinct checkin_ids): ', unique_column_count(foursqr, 0))
    print('c) Total sessions (distinct session_ids): ', unique_column_count(foursqr, 2))
    t = time.time()
    print('d) How many countries (distinct country_codes): ', unique_column_count(cities.map(lambda row: row[1]), 3))
    print('----> time: ', time.time() - t)
    t = time.time()
    print('e) How many cities (distinct city names): ', unique_column_count(cities.map(lambda row: row[1]), 0))
    print('----> time: ', time.time() - t)


def task_5(foursqr):
    a = time.time()
    session_lengths = foursqr.map(lambda row: (row[2], 1))\
                        .reduceByKey(lambda x, y: x+y)\
                        .map(lambda row: (row[1], ))\
                        .countByKey()
    # sez = {y: x for x, y in session_lengths.items()}
    print('Task 5 time: ', time.time() - a)
    '''
    Creating the most beautiful graph you have ever seen, now with even more sparkles.
    print(session_lengths)
    plt.bar(session_lengths.keys(), list(map(log, session_lengths.values())))
    plt.xlabel('Session length')
    plt.ylabel('Sessions of given length (in log)')
    plt.show()
    '''


def task_6(foursqr):
    selection = foursqr.map(lambda row: (row[2],
                                         {'pos': (float(row[5]), float(row[6]))}))\
        .groupByKey()\
        .filter(lambda row: len(row[1]) >= 4)\
        .map(lambda row: (row[0], haversine_path_dict(list(row[1]))))\
        .collect()

    '''
    for session_map in selection:
        print("%s\t%s\n" % (session_map[0], session_map[1]))
    '''


def task_7(foursqr):
    selection = foursqr.map(lambda row: (row[2], {'pos': (float(row[5]), float(row[6])),
                                                  'checkin_id': row[0],
                                                  'user_id': row[1],
                                                  'timestamp': str(timestamp(row[3], row[4])),
                                                  'category': row[7],
                                                  'subcategory': row[8]}
                                         )) \
        .groupByKey() \
        .map(lambda row: (row[0], row[1], haversine_path_dict(list(row[1])))) \
        .filter(lambda row: row[2] >= 50.0) \
        .takeOrdered(100, key=lambda row: -row[2])

    '''
    for session_id, session_store, length in selection:
        for session_map in session_store:
            print("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" % (session_map['checkin_id'],
                                                        session_map['user_id'],
                                                        session_id,
                                                        session_map['timestamp'],
                                                        session_map['pos'][0],
                                                        session_map['pos'][1],
                                                        session_map['category'],
                                                        session_map['subcategory']))

    with open("foursquare_task7_output.tsv", "w") as sessions_file:
        sessions_file.write("checkin_id\tuser_id\tsession_id\ttime\tlat\tlon\tcategory\tsubcategory\n")
        for session_id, session_store, length in selection:
            for session_map in session_store:
                sessions_file.write("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" % (session_map['checkin_id'],
                                                                          session_map['user_id'],
                                                                          session_id,
                                                                          session_map['timestamp'],
                                                                          session_map['pos'][0],
                                                                          session_map['pos'][1],
                                                                          session_map['category'],
                                                                          session_map['subcategory']))

    '''

if __name__ == "__main__":
    sc = SparkContext(appName="Foursquare")
    sqlContext = SQLContext(sc)

    print('Task 1 - Exploratory Analysis of Foursquare Dataset')
    print('Task 1.1 - Load the dataset')

    foursqr = sc.textFile('Foursquare_data/dataset_TIST2015.tsv')
    # foursqr = sc.textFile('foursquare_excerpt.tsv')
    # foursqr = sc.textFile('foursquare_excerpt2.tsv')
    # foursqr = sc.textFile('foursquare_excerpt3.tsv')
    # foursqr = sc.textFile('foursquare_excerpt4.tsv')
    header = foursqr.first()  # extract header
    foursqr = foursqr.filter(lambda x: x != header).map(lambda x: tuple(x.split('\t')))

    print('Foursquare loaded, amount of partions: %s' % (foursqr.getNumPartitions(),))

    cities_file = sc.textFile('Foursquare_data/dataset_TIST2015_Cities.txt').map(lambda x: tuple(x.split('\t')))
    print('Cities loaded')

    print('Creating SQL Context for cities')
    cities_df = cities_file.map(lambda c: Row(name=c[0], lat=float(c[1]), lon=float(c[2]), country_code=c[3],
                                           country_name=c[4], type=c[5]))

    cities_df = sqlContext.createDataFrame(cities_df)
    cities_df.registerTempTable('cities')
    sqlContext.cacheTable('cities')
    print('SQL context for cities created')

    # TODO : define as function
    # See timestamp function
    print('Task 1.2 - Calculate local time for each checkin')
    checkin_df = foursqr.map(lambda l: Row(checkin_id=int(l[0]), user_id=int(l[1]), session_id=l[2],
                                         time=timestamp(l[3], l[4]), lat=float(l[5]), lon=float(l[6]), category=l[7],
                                         subcategory=l[8])
                           )

    print('Local time for each checkin calculated')

    print('Creating SQL Context for checkins')
    checkin_df = sqlContext.createDataFrame(checkin_df)
    checkin_df.registerTempTable("checkins")

    print('SQL context for checkins created')

    print('Task 1.3 - Assign a city and country to each check-in')
    # returns a dataframe
    cities = task_3(checkin_df, cities_df)

    print('Task 1.4 - Bunch of questions')
    task_4(foursqr, cities)

    print('Task 1.5 - Calculate lengths of sessions as number of check-ins and provide a histogram')
    # task_5(foursqr)
    # TODO: create histogram

    print('Task 1.6 - Calculate distance in km for sessions with 4 check-ins or more')
    #task_6(foursqr)

    print('Task 1.7 - Find 100 longest sessions')
    #task_7(foursqr)
    # https://imp.cartodb.com/viz/803ad096-ed4a-11e5-a173-0e5db1731f59/public_map


    sc.stop()
