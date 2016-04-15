from __future__ import print_function
from pyspark import SparkContext, SQLContext, Row


# - - - - - - - - - - - - - HELPER FUNCTIONS - - - - - - - - - - - - -

def getWeekday(timestamp, offset):
    weekdays = ['Thursday', 'Friday', 'Saturday', 'Sunday', 'Monday', 'Tuesday', 'Wednesday']
    # Assuming offset is seconds, not minutes
    weekday = ((timestamp + offset) / 86400) % 7
    return weekdays[weekday]


def getTweetPolarity(tweet, negativeWords, positiveWords):
    wordList = tweet.translate(None, ",!.;#?:@-").lower().split(' ')
    numPositive = len([i for i in wordList if i in positiveWords])
    numNegative = len([i for i in wordList if i in negativeWords])

    return 1 if numPositive - numNegative > 0 else -1 if numPositive - numNegative < 0 else 0

# - - - - - - - - - - - - - MAIN FUNCTION - - - - - - - - - - - - -

if __name__ == "__main__":
    sc = SparkContext(appName="Twitter")
    sqlContext = SQLContext(sc)

    print('Task 2 - Sentiment Analysis on Twitter')

    print('Task 2.1 - Load the twitter dataset')

    # Loading files

    twitter_data = sc.textFile('twitter_tweets_example.tsv').map(lambda x: tuple(x.split('\t')))
    print('Tweets loaded')

    '''
    negative_words_data = sc.textFile('Twitter_data/negative-words.txt').flatMap(lambda x: x.split('\n'))
    print('Negative words loaded')

    positive_words_data = sc.textFile('Twitter_data/positive-words.txt').flatMap(lambda x: x.split('\n'))
    print('Positive words loaded')
    '''

    negative_words_data = []
    positive_words_data = []
    with open('Twitter_data/positive-words.txt', 'r') as text_file:
        positive_words_data = text_file.read().split('\n')
        positive_words_data = set(positive_words_data)

    with open('Twitter_data/negative-words.txt', 'r') as text_file:
        negative_words_data = text_file.read().split('\n')
        negative_words_data = set(negative_words_data)

    print('Positive and negative words loaded')

    print('Task 2.2 - Find aggregated polarity of all english tweets for each city in the US for each day of week')

    # timestamp[0], time_offset[1], country[2], place_type[3], city[4], lang[5], tweet[6]
    tweets = twitter_data.map(lambda t: (int(t[0]), int(t[8]), str(t[1]), str(t[3]), str(t[4]), str(t[5]), str(t[10]))) \
        .filter(lambda row: row[2] == 'United States'
                            and row[3] == 'city'
                            and row[5] == 'en')

    # city[0], dayOfWeek[1], polarityOfTweet[2]
    tweets = tweets.map(lambda t: (str(t[4]),
                                   str(getWeekday(t[0], t[1])),
                                   getTweetPolarity(str(t[6]), negative_words_data, positive_words_data)))

    # TODO : make key: combine city, weekday

    # TODO : combine by key and sum polarity

    tweets.foreach(print)

    print('Task 2.3 - Output result into file')
    # 'city<tab>day_of_week<tab>overall_polarity'
    # tweets.saveAsTextFile("task2.tsv")

    sc.stop()
