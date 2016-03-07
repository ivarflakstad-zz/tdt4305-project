from __future__ import print_function
from operator import add

from pyspark import SparkContext

if __name__ == "__main__":
    sc = SparkContext(appName="Foursquare")
    foursqr = sc.textFile('test.tsv', 1)
    foursqr = foursqr.map(lambda x: tuple(x.split('\t')))

    # Unique users
    tally = foursqr.map(lambda x: x[1]).distinct().count()
    print('Distinct user_ids', tally)

    sc.stop()