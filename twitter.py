from pyspark import SparkContext, SQLContext

if __name__ == "__main__":
    sc = SparkContext(appName="Twitter")
    sqlContext = SQLContext(sc)

    print('Task 2 - Sentiment Analysis on Twitter')

    print('Task 1 - Load the twitter dataset')

    # Loading files

    # twitter_data = sc.textFile('Twitter_data/geotweets.tsv')
    twitter_data = sc.textFile('twitter_tweets_example.tsv').map(lambda x: tuple(x.split('\t')))
    print('Tweets loaded')

    negative_words_data = sc.textFile('Twitter_data/negative-words.txt').flatMap(lambda x: x.split('\n'))
    print('Negative words loaded')

    positive_words_data = sc.textFile('Twitter_data/positive-words.txt').flatMap(lambda x: x.split('\n'))
    print('Positive words loaded')

    print('Task 2 - Find aggregated polarity of all english tweets for each city in the US for each day of week')

    print('Task 3 - Output result into file')
    # Format: city<tab>day_of_week<tab>overall_polarity