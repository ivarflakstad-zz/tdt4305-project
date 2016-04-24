from __future__ import print_function
from pyspark import SparkContext, SQLContext, Row
import sys


# - - - - - - - - - - - - - HELPER FUNCTIONS - - - - - - - - - - - - -

def get_weekday(timestamp, offset):
    weekdays = ['Thursday', 'Friday', 'Saturday', 'Sunday', 'Monday', 'Tuesday', 'Wednesday']
    # Assuming offset is seconds, not minutes
    weekday = ((timestamp + offset) / 86400) % 7
    return weekdays[weekday]


def tweet_char_filter(tweet):
    tweet_list = []
    tweet_part = ''
    for char in tweet.lower():
        order = ord(char)
        if 64 < order < 91 or 96 < order < 123 or char == 32 or char == 45:
            tweet_part += char
        else:
            tweet_list.append(tweet_part)
            tweet_part = ''
    return tweet_list

def get_tweet_polarity(tweet, negative_words, positive_words):
    word_list = tweet_char_filter(tweet)
    num_positive = len([i for i in word_list if i in positive_words])
    num_negative = len([i for i in word_list if i in negative_words])

    return 1 if num_positive - num_negative > 0 else -1 if num_positive - num_negative < 0 else 0


def createCombinedKey(weekday, city):
    return ''.join([weekday," ",city])

# - - - - - - - - - - - - - MAIN FUNCTION - - - - - - - - - - - - -

if __name__ == "__main__":
    sc = SparkContext(appName="Twitter")
    sqlContext = SQLContext(sc)

    print('Task 2 - Sentiment Analysis on Twitter')

    print('Task 2.1 - Load the twitter dataset')

    # input_path = sys.argv[1]
    # output_ath = sys.argv[2]
    # pos_path = sys.argv[3]
    # neg_path = sys.argv[4]

    # Loading files

    # twitter_data = sc.textFile(input_path).map(lambda x: tuple(x.split('\t')))
    twitter_data = sc.textFile('tweets_excerpt.tsv').map(lambda x: tuple(x.split('\t')))
    # twitter_data = sc.textFile('twitter_data/geotweets.tsv').map(lambda x: tuple(x.split('\t')))
    print('Tweets loaded')

    # positive_words_data = sc.textFile(pos_path).collect()
    positive_words_data = sc.textFile("hdfs://dascosa09.idi.ntnu.no:8020/user/janryb/positive-words.txt").collect()
    print('Positive words loaded')

    # negative_words_data = sc.textFile(neg_path).collect()
    negative_words_data = sc.textFile("hdfs://dascosa09.idi.ntnu.no:8020/user/janryb/negative-words.txt").collect()
    print('Negative words loaded')

    print('Task 2.2 - Find aggregated polarity of all english tweets for each city in the US for each day of week')

    # timestamp[0], time_offset[1], country[2], place_type[3], city[4], lang[5], tweet[6]
    tweets = twitter_data.map(
            lambda t: (int(t[0]), int(t[8]), str(t[1]), str(t[3]), str(t[4]), str(t[5]), str(t[10]))).filter(
            lambda t: t[2] == 'United States' and t[3] == 'city' and t[5] == 'en')

    # city[0], dayOfWeek[1], polarityOfTweet[2]
    tweets = tweets.map(lambda t: (
        str(t[4]), str(get_weekday(t[0], t[1])), get_tweet_polarity(str(t[6]), negative_words_data, positive_words_data)))

    # Combine city and weekday
    tweets = tweets.map(lambda t: (createCombinedKey(t[0], t[1]), t[2]))

    # Sum polarity on (city, weekday)
    tweets = tweets.combineByKey(lambda value: (value, 1),
                             lambda x, value: (x[0] + value, x[1] + 1),
                             lambda x, y: (x[0] + y[0], x[1] + y[1])) \
        .map(lambda (label, (value_sum, count)): (label, value_sum))

    # TODO : sort by city, weekdays

    for session_map in tweets.collect():
        print("%s\t%s\n" % (session_map[0], session_map[1]))

    print('Task 2.3 - Output result into file')
    # 'city<tab>day_of_week<tab>overall_polarity'
    # tweets.saveAsTextFile('output_path')

    sc.stop()
