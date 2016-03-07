from __future__ import print_function
from operator import add

from pyspark import SparkContext

if __name__ == "__main__":
    # Setting context because Spark requires it
    sc = SparkContext(appName="Foursquare")

    # Loading file
    foursqr = sc.textFile('test.tsv', 1)

    """
    Mapping tsv to list of tuples:

    checkin_id	user_id	session_id	utc_time	timezone_offset	lat	lon	category	subcategory
    25915736	1539	1539_AE_117	2013-02-06 17:46:30	-360	25.254118	55.30333	Food	Sushi Restaurant
    22889371	1539	1539_AE_18	2012-11-11 12:29:30	-360	25.196387	55.280058	Outdoors & Recreation	Athletics & Sports

    is mapped to ->

    [(checkin_id, user_id, session_id, utc_time, timezone_offset, lat, lon, category, subcategory),
    (25915736, 1539, 1539_AE_117, 2013-02-06 17:46:30, -360, 25.254118, 55.30333, Food, Sushi Restaurant),
    (22889371, 1539, 1539_AE_18, 2012-11-11 12:29:30, -360, 25.196387, 55.280058, Outdoors & Recreation, Athletics & Sports)
    ]
    """
    foursqr = foursqr.map(lambda x: tuple(x.split('\t')))

    # Tallying for unique users.
    column_2 = foursqr.map(lambda row: row[1])  # Choosing only from column #2, same as the user_id field
    only_uniq = column_2.distinct()             # Keep only the unique user_id's
    tally = only_uniq.count()                   # Count the amount of unique user_id's
    print('Distinct user_ids', tally)           # WOOP WOOP

    sc.stop()