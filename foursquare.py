from __future__ import print_function
from operator import add

from pyspark import SparkContext
from pyspark.sql import SQLContext, Row

from datetime import datetime, timedelta
from math import radians, cos, sin, asin, sqrt


def haversine(lon1, lat1, lon2, lat2):
    """
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees)
    """
    # convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    # haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * asin(sqrt(a))
    r = 6371  # Radius of earth in kilometers. Use 3956 for miles
    return c * r


def timestamp(stamp, tz):
    date, time = stamp.split(' ')
    year, month, day = map(int, date.split('-'))
    hour, minute, sec = map(int, time.split(':'))
    return datetime(year=year, month=month, day=day, hour=hour, minute=minute, second=sec) + timedelta(minutes=int(tz))


def tsv_unique_count(sc_file, col_n=0, split_c='\t'):
    """
    Count unique items in a Tabular Separated Values file by column.
    :param sc_file: SparkContext File, preferably TSV
    :param col_n: Column to count
    :param split_c: Character to split on, defaults to tab.
    :return:
    """

    '''
    Mapping tsv to list of tuples:

    checkin_id	user_id	session_id	utc_time	timezone_offset	lat	lon	category	subcategory
    25915736	1539	1539_AE_117	2013-02-06 17:46:30	-360	25.254118	55.30333	Food	Sushi Restaurant
    22889371	1539	1539_AE_18	2012-11-11 12:29:30	-360	25.196387	55.280058	Outdoors & Recreation	Athletics & Sports

    is mapped to ->

    [(checkin_id, user_id, session_id, utc_time, timezone_offset, lat, lon, category, subcategory),
    (25915736, 1539, 1539_AE_117, 2013-02-06 17:46:30, -360, 25.254118, 55.30333, Food, Sushi Restaurant),
    (22889371, 1539, 1539_AE_18, 2012-11-11 12:29:30, -360, 25.196387, 55.280058, Outdoors & Recreation, Athletics & Sports)
    ]
    '''
    foursqr = sc_file.map(lambda x: tuple(x.split(split_c)))

    # Tallying for unique users.
    column = foursqr.map(lambda row: row[col_n])  # Choosing only from column #2, same as the user_id field
    only_uniq = column.distinct()  # Keep only the unique user_id's
    tally = only_uniq.count()  # Count the amount of unique user_id's
    return tally


def csv_unique_count(sc_file, col_n=0):
    """
    Count unique items in a Comma Separated Values file by column.
    :param sc_file: SparkContext File, preferably CSV
    :param col_n: Column to count
    :return:
    """
    tsv_unique_count(sc_file, col_n, split_c='\c')


if __name__ == "__main__":
    # Setting context because Spark requires it
    sc = SparkContext(appName="Foursquare")
    sqlContext = SQLContext(sc)

    print('Foursquare!')
    print('Task 1 - load the dataset')
    # Loading file
    foursqr = sc.textFile('test.tsv')
    header = foursqr.first()  # extract header
    foursqr = foursqr.filter(lambda x: x != header)  # filter away header
    print('foursquare loaded')

    cities_file = sc.textFile('cities.txt')
    print('cities loaded')

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


    print('Task 3')
    print('needs to be done')


    all_checkins = sqlContext.sql("SELECT * FROM checkins")
    wat = all_checkins.map(lambda l: "%i, %i, %s, %s, %s, %s, %s" % (l.checkin_id, l.user_id, l.session_id, l.time,
                                                                     (l.lat, l.lon), l.category, l.subcategory))
    for all in wat.collect():
        print(all)

    all_cities = sqlContext.sql("SELECT * FROM cities")
    wat = all_cities.map(lambda c: "%s, %s, %s, %s, %s" % (c.name, (c.lat, c.lon), c.country_code, c.country_name, c.type))
    for all in wat.collect():
        print(all)

    print('Task 4:')
    print('a) Unique users (distinct user_ids): ', tsv_unique_count(foursqr, 1))
    print('b) Total checkins (distinct checkin_ids): ', tsv_unique_count(foursqr, 0))
    print('c) Total sessions (distinct session_ids): ', tsv_unique_count(foursqr, 2))
    print('d) How many countries (distinct country_codes): ', tsv_unique_count(cities_file, 3))
    print('e) How many cities (distinct city names): ', tsv_unique_count(cities_file, 0))

    print('Task 5')
    print('needs to be done')

    print('Task 6')
    print('needs to be done')

    print('Task 7')
    print('needs to be done')

    print('Task 8')
    print('optional')

    sc.stop()
