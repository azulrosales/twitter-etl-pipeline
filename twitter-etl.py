import tweepy # for accessing the Twitter API
import pandas as pd

def run_twitter_etl():

    access_key = 'xx'
    access_secret = 'xx'
    consumer_key = 'xx'
    consumer_secret = 'xx'

    # Twitter authentication
    auth = tweepy.OAuthHandler(access_key, access_secret) # OAuth: standard authorization protocol
    auth.set_access_token(consumer_key, consumer_secret)

    # Creating an API object
    api = tweepy.API(auth)

    # Retrieving most recent tweets posted by the user 
    tweets = api.user_timeline(screen_name='@elonmusk', 
                                count=200, # 200 is the max allowed
                                include_rts=False, # excludes retweets
                                tweet_mode='extended' # extracts the full text
                                )

    # Formatting and storing data into list
    tweets_list = []
    for tweet in tweets:

        refined_tweet = {
            'user' : tweet.user.screen_name,
            'text' : tweet._json['full_text'],
            'favorite_count' : tweet.favorite_count,
            'retweet_count' : tweet.retweet_count,
            'created_at' : tweet.created_at
        }

        tweets_list.append(refined_tweet)

    df = pd.DataFrame(tweets_list)
    df.to_csv('s3://twitter-etl-bucket/elon-musk-tweets.csv')