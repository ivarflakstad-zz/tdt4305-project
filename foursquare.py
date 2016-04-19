from __future__ import print_function
from operator import add

from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
import pyspark.sql.functions as pyspark_f
from pyspark.sql.types import LongType, ArrayType

import time
from datetime import datetime, timedelta
from math import radians, cos, sin, asin, sqrt


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

    cart_user_city = user_coordination_data.cartesian(city_coordination_data).map(
        lambda ((checkin_id, checkin_lat, checkin_lon), (city, lat, lon, country)): (
        checkin_id, city, country, haversine(float(checkin_lat), float(checkin_lon), float(lat), float(lon)),
        checkin_lat, checkin_lon)).filter(lambda (id, city, country, d, checkin_lat, checkin_lon): d < 1)

    print(cart_user_city.collect())


def task_4(foursqr, cities):
    a = time.time()
    print('a) Unique users (distinct user_ids): ', unique_column_count(foursqr, 1))
    print('b) Total checkins (distinct checkin_ids): ', unique_column_count(foursqr, 0))
    print('c) Total sessions (distinct session_ids): ', unique_column_count(foursqr, 2))
    print('d) How many countries (distinct country_codes): ', unique_column_count(cities_file, 3))
    print('e) How many cities (distinct city names): ', unique_column_count(cities_file, 0))
    print('time:', time.time() - a)


def task_5(foursqr):
    a = time.time()
    session_lengths = foursqr.map(lambda row: row[2]).countByValue()
    # sez = {y: x for x, y in session_lengths.items()}
    print(session_lengths)
    print('time 1:', time.time() - a)
    # print(session_lengths)

    '''
    a = time.time()
    sessionz = foursqr.map(lambda row: row[2]).groupBy(lambda x: x).mapValues(len).map(lambda x: (x[1], x[0]))\
        .groupByKey().mapValues(len).collect()
    print(sessionz)
    print('time 2:', time.time() - a)
    '''


def task_6(foursqr):
    a = time.time()
    selection = foursqr.map(lambda row: (row[2], {'pos': (float(row[5]), float(row[6]))})).groupByKey().filter(
        lambda row: len(row[1]) >= 4).map(lambda row: (row[0], haversine_path_dict(list(row[1])))).collect()
    print(selection)
    print('time 1:', time.time() - a)


def task_7(foursqr, df):
    a = time.time()
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
    print('time 1:', time.time() - a)
    print('hurro')
    print(selection)

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

    # TODO : output to .tsv/.csv file and visualize in CartoDB

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

    with open("foursquare_sessions_example.tsv", "w") as sessions_file:
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
    print('Task 1.1 - load the dataset')

    # foursqr = sc.textFile('Foursquare_data/dataset_TIST2015.tsv')
    foursqr = sc.textFile('foursquare_sessions_example.tsv')
    header = foursqr.first()  # extract header
    foursqr = foursqr.filter(lambda x: x != header).map(lambda x: tuple(x.split('\t')))
    print('Foursquare loaded')

    cities_file = sc.textFile('foursquare_cities_example.txt').map(lambda x: tuple(x.split('\t')))
    print('Cities loaded')

    print('Creating SQL Context')
    # Creating SQL context
    cities = cities_file.map(lambda c: Row(name=c[0], lat=float(c[1]), lon=float(c[2]), country_code=c[3],
                                           country_name=c[4], type=c[5]))

    cities_df = sqlContext.createDataFrame(cities)
    cities_df.registerTempTable('cities')
    sqlContext.cacheTable('cities')

    # TODO : define as function
    # See timestamp function
    print('Task 1.2 - Calculate local time for each checkin')
    checkins = foursqr.map(lambda l: Row(checkin_id=int(l[0]), user_id=int(l[1]), session_id=l[2],
                                         time=timestamp(l[3], l[4]), lat=float(l[5]), lon=float(l[6]), category=l[7],
                                         subcategory=l[8], city=closest_city(float(l[5]), float(l[6])))
                           )

    checkin_df = sqlContext.createDataFrame(checkins)
    checkin_df.registerTempTable("checkins")
    print('SQL Context created')

    print('Task 1.3 - Assign a city and country to each check-in')
    # task_3(foursqr, cities_file)

    print('Task 1.4 - bunch of questions')
    # task_4(foursqr, cities_file)

    print('Task 1.5 - Calculate lengths of sessions as number of check-ins and provide a histogram')
    # task_5(foursqr)
    # TODO: create histogram

    print('Task 1.6 - Calculate distance in km for sessions with 4 check-ins or more')
    # task__6(foursqr)

    print('Task 1.7 - Find 100 longest sessions')
    # task_7(foursqr, checkin_df)

    sc.stop()
