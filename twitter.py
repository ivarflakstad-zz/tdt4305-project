import nltk

from pyspark import SparkContext, SQLContext, Row
from nltk.tokenize import word_tokenize

nltk.download('punkt')

def dayOfWeek(timestamp, offset):
    weekdays = ['Thursday', 'Friday', 'Saturday', 'Sunday', 'Monday', 'Tuesday', 'Wednesday']
    # Assuming offset is seconds, not minutes
    weekday = ((timestamp + offset)/86400) % 7
    return weekdays[weekday]

if __name__ == "__main__":
    sc = SparkContext(appName="Twitter")
    sqlContext = SQLContext(sc)

    print('Task 2 - Sentiment Analysis on Twitter')

    print('Task 2.1 - Load the twitter dataset')

    # Loading files

    twitter_data = sc.textFile('twitter_tweets_example.tsv').map(lambda x: tuple(x.split('\t')))
    print('Tweets loaded')

    negative_words_data = sc.textFile('Twitter_data/negative-words.txt').flatMap(lambda x: x.split('\n'))
    print('Negative words loaded')

    positive_words_data = sc.textFile('Twitter_data/positive-words.txt').flatMap(lambda x: x.split('\n'))
    print('Positive words loaded')

    print('Task 2.2 - Find aggregated polarity of all english tweets for each city in the US for each day of week')

    # timestamp[0], time_offset[1], country[2], place_type[3], city[4], lang[5], tweet[6]
    tweets = twitter_data.map(lambda t: (int(t[0]), int(t[8]), str(t[1]), str(t[3]), str(t[4]), str(t[5]), str(t[10]))) \
        .filter(lambda row: row[5] == 'en') \
        .filter(lambda row: row[3] == 'city') \
        .filter(lambda row: row[2] == 'United States')

    # city[0], dayOfWeek[1], tweet[2]
    tweets = tweets.map(lambda t: (str(t[4]), str(dayOfWeek(t[0],t[1])), str(t[6])))

    print (tweets.take(10))

    print('Task 2.3 - Output result into file')
    # result.saveAsTextFile("task2.tsv") 'city<tab>day_of_week<tab>overall_polarity'

    sc.stop()