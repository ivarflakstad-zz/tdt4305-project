from __future__ import print_function
from operator import add

from pyspark import SparkContext
from pyspark.sql import SQLContext, Row

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
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

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
    :param split_c: Character to split on, defaults to tab.
    :return:
    """
    return sc_file.map(lambda row: row[col_n]).distinct().count()


if __name__ == "__main__":
    sc = SparkContext(appName="Foursquare")
    sqlContext = SQLContext(sc)

    print('Foursquare!')
    print('Task 1 - load the dataset')
    # Loading file
    foursqr = sc.textFile('test.tsv')
    header = foursqr.first()  # extract header
    foursqr = foursqr.filter(lambda x: x != header).map(lambda x: tuple(x.split('\t')))
    print('foursquare loaded')

    cities_file = sc.textFile('cities.txt').map(lambda x: tuple(x.split('\t')))
    print('cities loaded')

    '''
    print('Creating SQL Context')
    # Creating SQL context
    rows = foursqr.map(lambda l: l.split('\t'))
    print('Task 2 - calculate local time for each checkin')  # see timestamp function
    checkins = rows.map(lambda l: Row(checkin_id=int(l[0]), user_id=int(l[1]), session_id=l[2],
                                      time=timestamp(l[3], l[4]), lat=float(l[5]), lon=float(l[6]), category=l[7],
                                      subcategory=l[8]))

    schemaCheckins = sqlContext.createDataFrame(checkins)
    schemaCheckins.registerTempTable("checkins")

    c_rows = cities_file.map(lambda l: l.split('\t'))
    cities = c_rows.map(lambda c: Row(name=c[0], lat=float(c[1]), lon=float(c[2]), country_code=c[3],
                                      country_name=c[4], type=c[5]))

    schemaCities = sqlContext.createDataFrame(cities)
    schemaCities.registerTempTable("cities")

    print('SQL Context created')
    '''

    print('Task 3')

    print('needs to be done')

    print('Task 4:')
    print('a) Unique users (distinct user_ids): ', unique_column_count(foursqr, 1))
    print('b) Total checkins (distinct checkin_ids): ', unique_column_count(foursqr, 0))
    print('c) Total sessions (distinct session_ids): ', unique_column_count(foursqr, 2))
    print('d) How many countries (distinct country_codes): ', unique_column_count(cities_file, 3))
    print('e) How many cities (distinct city names): ', unique_column_count(cities_file, 0))

    print('Task 5')
    session_lengths = foursqr.map(lambda row: row[2]).countByValue()
    '''
    import matplotlib.pyplot as plt
    x = range(len(test.keys()))
    plt.bar(x, test.values(), 2, color='g')
    plt.savefig("stuff.svg")
    '''
    print(session_lengths)
    print('Need to create some sort of histogram')

    print('Task 6')
    test = foursqr.map(lambda row: (row[2], {'pos': (float(row[5]), float(row[6]))})) \
        .groupByKey() \
        .filter(lambda row: len(row[1]) >= 4) \
        .map(lambda row: (row[0], haversine_path_dict(list(row[1]))))

    print(test.collect())

    print('Task 7')
    test = foursqr.map(lambda row: (row[2], {'pos': (float(row[5]), float(row[6])),
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

    with open("sessions.tsv", "w") as sessions_file:
        sessions_file.write("checkin_id\tuser_id\tsession_id\ttime\tlat\tlon\tcategory\tsubcategory\n")
        for session_id, session_store, length in test:
            for session_map in session_store:
                sessions_file.write("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" % (session_map['checkin_id'],
                                                                          session_map['user_id'],
                                                                          session_id,
                                                                          session_map['timestamp'],
                                                                          session_map['pos'][0],
                                                                          session_map['pos'][1],
                                                                          session_map['category'],
                                                                          session_map['subcategory']))

    print('needs to be done')

    print('Task 8')
    print('optional')

    sc.stop()
