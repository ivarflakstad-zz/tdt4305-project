from __future__ import print_function
from pyspark import SparkContext, SQLContext, Row
import re, sys


# - - - - - - - - - - - - - HELPER FUNCTIONS - - - - - - - - - - - - -

def get_day_of_week(timestamp, offset):
    weekdays = ['3', '4', '5', '6', '0', '1', '2']
    # Assuming offset is seconds, not minutes
    weekday = ((timestamp + offset) / 86400) % 7
    return weekdays[weekday]


def get_weekday(day_of_week):
    weekdays = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    return weekdays[int(day_of_week)]


def tweet_char_filter(tweet):
    # Removes all special characters, leaves only lowercase a-z
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


def createCombinedKey(city, weekday):
    return ''.join([city, " ", weekday])


def getCity(combinedKey):
    return combinedKey.split(',')[0]


def getWeekday(combinedKey):
    return combinedKey.split(' ')[-1]


# - - - - - - - - - - - - - MAIN FUNCTION - - - - - - - - - - - - -

if __name__ == "__main__":
    sc = SparkContext(appName="Twitter")
    sqlContext = SQLContext(sc)

    print('Task 2 - Sentiment Analysis on Twitter')

    print('Task 2.1 - Load the twitter dataset')

    # Set the paths of <inputfile> <outputfile> <positive_words> <negative_words>
    if len(sys.argv) == 4:
        input_path = sys.argv[1]
        output_path = sys.argv[2]
        pos_path = sys.argv[3]
        neg_path = sys.argv[4]
    elif len(sys.argv) == 2:
        input_path = sys.argv[1]
        output_path = sys.argv[2]
        pos_path = "hdfs://dascosa09.idi.ntnu.no:8020/user/janryb/positive-words.txt"
        neg_path = "hdfs://dascosa09.idi.ntnu.no:8020/user/janryb/negative-words.txt"
    else:
        print('twitter.py <inputfile> <outputfile> <positive_words> <negative_words>')

        # For testing:
        input_path = "tweets_excerpt.tsv"
        output_path = "twitter_output.tsv"
        pos_path = "hdfs://dascosa09.idi.ntnu.no:8020/user/janryb/positive-words.txt"
        neg_path = "hdfs://dascosa09.idi.ntnu.no:8020/user/janryb/negative-words.txt"

    # Loading files
    twitter_data = sc.textFile(input_path).map(lambda x: tuple(x.split('\t')))
    print('Tweets loaded')

    positive_words_data = sc.textFile(pos_path).collect()
    print('Positive words loaded')

    negative_words_data = sc.textFile(neg_path).collect()
    print('Negative words loaded')

    print('Task 2.2 - Find aggregated polarity of all english tweets for each city in the US for each day of week')

    # Filter out english tweets from cities in the US
    tweets = twitter_data.map(
            lambda t: (int(t[0]), int(t[8]), str(t[1]), str(t[3]), str(t[4]), str(t[5]), str(t[10]))).filter(
            lambda t: t[2] == 'United States' and t[3] == 'city' and t[5] == 'en')

    # Find the polarity of single tweets (-1, 0 or 1)
    tweets = tweets.map(lambda t: (
        str(t[4]), str(get_day_of_week(t[0], t[1])),
        get_tweet_polarity(str(t[6]), negative_words_data, positive_words_data)))

    # Create combined key with (city, weekday)
    tweets = tweets.map(lambda t: (createCombinedKey(t[0], t[1]), t[2]))

    # Sum polarity on (city, weekday)
    tweets = tweets.combineByKey(lambda value: (value, 1), lambda x, value: (x[0] + value, x[1] + 1),
                                 lambda x, y: (x[0] + y[0], x[1] + y[1])).map(
        lambda (label, (value_sum, count)): (label, value_sum)).map(lambda t: (getCity(t[0]), getWeekday(t[0]), t[1]))

    # Sort by city -> weekday
    tweets = tweets.sortBy(lambda x: (-1 * x[1], x[0]))

    print('Task 2.3 - Output result into file')

    with open(output_path, "w") as sessions_file:
        sessions_file.write("city\tweekday\toverall sentiment\n")
        for tweet in tweets.collect():
            sessions_file.write("%s\t%s\t%s\n" % (tweet[0], get_weekday(tweet[1]), tweet[2]))

    print('Output completed')

    sc.stop()
