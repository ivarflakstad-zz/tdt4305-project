from __future__ import print_function
from operator import add

from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
import pyspark.sql.functions as pyspark_f
from pyspark.sql.types import LongType, ArrayType
import matplotlib.pyplot as plt
import math

import time
from datetime import datetime, timedelta
from math import radians, cos, sin, asin, atan2, sqrt, degrees


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


def multi_column_count(sc_file):
    return sc_file.map(lambda row: (row[0], row[1], row[2])).groupByKey()


def closest_city(lat, lon):
    return 1


def task_3(foursqr, cities):
    # checkin_id[0], lat[1], lon[2]
    user_coordination_data = foursqr.map(lambda x: (x[0], x[5], x[6]))

    # city[0], lat[1], lon[2], country[3]
    city_coordination_data = cities.map(lambda x: (x[0], x[1], x[2], x[4]))

    cart_user_city = user_coordination_data.cartesian(city_coordination_data)\
          .map(lambda ((checkin_id, checkin_lat, checkin_lon), (city, lat, lon, country)):
             (checkin_id, (city, country,
              haversine(float(checkin_lat), float(checkin_lon), float(lat), float(lon)),
              checkin_lat, checkin_lon)))\
          .reduceByKey(lambda x1, x2: min(x1, x2, key=lambda x: x[1][2]))\
          .collect()

    for session_map in cart_user_city:
        print("%s\t%s\t%s\t%s\t%s\t%s\n" % (
            session_map[0], session_map[1][0], session_map[1][1], session_map[1][2], session_map[1][3], session_map[1][4]))


def task_4(foursqr):
    print('a) Unique users (distinct user_ids): ', unique_column_count(foursqr, 1))
    print('b) Total checkins (distinct checkin_ids): ', unique_column_count(foursqr, 0))
    print('c) Total sessions (distinct session_ids): ', unique_column_count(foursqr, 2))
    print('d) How many countries (distinct country_codes): ', unique_column_count(cities_file, 3))
    print('e) How many cities (distinct city names): ', unique_column_count(cities_file, 0))


def task_5(foursqr):
    session_lengths = foursqr.map(lambda row: (row[2], 1))\
                        .reduceByKey(lambda x, y: x+y)\
                        .map(lambda row: (row[1], ))\
                        .countByKey()
    # sez = {y: x for x, y in session_lengths.items()}
    print(session_lengths)

def task_6(foursqr):
    selection = foursqr.map(lambda row: (row[2],
                                         {'pos': (float(row[5]), float(row[6]))}))\
        .groupByKey()\
        .filter(lambda row: len(row[1]) >= 4)\
        .map(lambda row: (row[0], haversine_path_dict(list(row[1]))))\
        .collect()

    for session_map in selection:
        print("%s\t%s\n" % (session_map[0], session_map[1]))


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
    Implemented a reduceByKey version that is 4x as fast, however - it is also wrong as haversine demands ordered positions.


    a = time.time()

    def jeez(x, y):
        if x[1]:
            x[2] += haversine(x[1][-1][4], x[1][-1][5], y[0][4], y[0][5])
        else:
            x[2] += haversine(x[0][4], x[0][5], y[0][4], y[0][5])
        x[1] += [y[0]]
        x[3] += 1
        return x
    #lambda x, y: x + y + (haversine(x[0][4], x[0][5], y[4], y[5]), )) \
    selection = foursqr.map(lambda row: (row[2], [row[:2] + row[3:], [], 0, 0]))\
        .reduceByKey(jeez) \
        .filter(lambda row: row[1][-2] >= 50.0) \
        .takeOrdered(100, key=lambda row: -row[1][-2])
    print('time 2:', time.time() - a)
    print('hurro')
    print(selection)

    def stuff(i):
        return [i] if i else None

    test = pyspark_f.UserDefinedFunction(lambda i: int(i) if i else None, LongType())

    a = time.time()
    selection = df.select('*').collect()
    print('time 2:', time.time() - a)
    print('hurro 2')
    # print(selection)
    map(lambda row: (row[2], {'pos': (float(row[5]), float(row[6])),
                                                  'checkin_id': row[0],
                                                  'user_id': row[1],
                                                  'timestamp': str(timestamp(row[3], row[4])),
                                                  'category': row[7],
                                                  'subcategory': row[8]}
                                         ))\
        .groupByKey() \
        .map(lambda row: (row[0], row[1], haversine_path_dict(list(row[1])))) \
        .filter(lambda row: row[2] >= 50.0) \
        .takeOrdered(100, key=lambda row: -row[2])

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

    '''


import kdtree
from rtree import Rtree

def cartesian_to_latlong(x, y, z, r=6371.0):
    lat = asin(z / r)
    lon = atan2(y, x)
    return degrees(lat), degrees(lon)

def cartesian_haversine(node, point):
    x, y, z = node
    x1, y1, z1 = point
    lat, lon = cartesian_to_latlong(x, y, z)
    lat1, lon1 = cartesian_to_latlong(x1, y1, z1)
    return haversine(lat1, lon, lat1, lon1)


def latlong_to_cartesian(lat, lon, r=6371):
    lat = radians(lat)
    lon = radians(lon)

    x = r*cos(lon)*sin(lat)
    y = r*sin(lon)*sin(lat)
    z = r*cos(lat)
    return x, y, z

def kdtree_query(tree, lat, lon):
    coords = latlong_to_cartesian(float(lat), float(lon))
    result = tuple(tree.search_nn(coords))[0].data
    return result

def rtree_query(index, lat, lon):
    return index.nearest(latlong_to_cartesian(lat, lon))



def test(foursqr, cities):
    # checkin_id[0], lat[1], lon[2]
    user_coordination_data = foursqr.map(lambda x: (x[0], float(x[5]), float(x[6])))

    # city[0], lat[1], lon[2], country[3]
    city_coordination_data = cities.map(lambda x: (x[0], x[1], x[2], x[4]))
    a = time.time()
    cartesian_coords_cities_dict = dict(city_coordination_data
                                        .map(lambda row: (tuple(latlong_to_cartesian(float(row[1]), float(row[2]))), (row[0], row[3])))
                                        .collect())

    tree = kdtree.create(cartesian_coords_cities_dict.keys())
    cart_user_city_1 = user_coordination_data.map(lambda row:
                                                  (
                                                      cartesian_coords_cities_dict[
                                                          kdtree_query(tree, float(row[1]), float(row[2]))
                                                      ]
                                                      , row
                                                  )
                                                  ).collect()

    print("KDTree: ", time.time() - a)

    a = time.time()
    r_tree = Rtree()
    for key, value in cartesian_coords_cities_dict.items():
        r_tree.add(key, value)


    cart_user_city_2 = user_coordination_data.map(lambda row:
                                                  (
                                                      cartesian_coords_cities_dict[
                                                          rtree_query(r_tree, float(row[1]), float(row[2]))
                                                      ]
                                                      , row
                                                  )
                                                  ).collect()
    print("R-Tree: ", time.time() - a)
    '''
    a = time.time()
    cart_user_city_3 = user_coordination_data.cartesian(city_coordination_data)\
          .map(lambda ((checkin_id, checkin_lat, checkin_lon), (city, lat, lon, country)):
             (checkin_id, (city, country,
              haversine(float(checkin_lat), float(checkin_lon), float(lat), float(lon)),
              checkin_lat, checkin_lon)))\
          .reduceByKey(lambda x1, x2: min(x1, x2, key=lambda x: x[1][2]))\
          .collect()

    print("reduceByKey: ", time.time() - a)
    for session_map in cart_user_city_1:
        print('KDTree', session_map)
        #print("%s\t%s\t%s\t%s\t%s\t%s\n" % (
        #    session_map[0], session_map[1][0], session_map[1][1], session_map[1][2], session_map[1][3], session_map[1][4]))

    for session_map in cart_user_city_3:
        print('reduceByKey', session_map)
        #print("%s\t%s\t%s\t%s\t%s\t%s\n" % (
        #    session_map[0], session_map[1][0], session_map[1][1], session_map[1][2], session_map[1][3], session_map[1][4]))
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
    foursqr = foursqr.filter(lambda x: x != header).map(lambda x: tuple(x.split('\t'))).repartition(6)
    print('Foursquare loaded')

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
    # cities = task_3(foursqr, cities_file)

    print('Task 1.4 - Bunch of questions')
    # task_4(cities)

    print('Task 1.5 - Calculate lengths of sessions as number of check-ins and provide a histogram')
    task_5(foursqr)
    # TODO: create histogram

    print('Task 1.6 - Calculate distance in km for sessions with 4 check-ins or more')
    #task_6(foursqr)

    print('Task 1.7 - Find 100 longest sessions')
    #task_7(foursqr)
    # https://imp.cartodb.com/viz/803ad096-ed4a-11e5-a173-0e5db1731f59/public_map


    #test(foursqr, cities_file)


    sc.stop()
