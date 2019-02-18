# importing packages
from abc import ABC, abstractmethod
import pandas as pd
import time


# Generic TwitterAPI abstract class with all the methods defined
class TwitterAPI(ABC):

    # put the DB connection in the constructor so it can be reused
    def __init__(self, con):
        self.con = con
        super().__init__()

    @abstractmethod
    def post_tweets(self, broadcast):
        pass

    @abstractmethod
    def add_followers(self):
        pass

    @abstractmethod
    def add_following(self):
        pass

    @abstractmethod
    def get_timeline(self, user_id, broadcast):
        pass

    @abstractmethod
    def get_followers(self, user_id):
        pass

    @abstractmethod
    def get_followees(self, user_id):
        pass

    @abstractmethod
    def clear_db(self):
        pass

# Redis API Class that inherits from the TwitterAPI Abstract Class
class RedisAPI(TwitterAPI):

    def __init__(self,con):
        super().__init__(con)

    # Loads CSV containing users and whom they follow and loads that
    def add_followers(self):
        followers = pd.read_csv("C:/Users/bingb/Google Drive/DS4300/followers.csv")
        followers = followers.drop('Unnamed: 0', axis=1)
        for i in range(followers['user_id'].min(), followers['user_id'].max()):
            key = "followers:" + str(i)
            vals = followers.loc[followers['follows_id'] == i]['user_id'].tolist()
            self.con.sadd(key, *vals)

    # Loads CSV containing users and whom they follow and loads that
    def add_following(self):
        followers = pd.read_csv("C:/Users/bingb/Google Drive/DS4300/followers.csv")
        followers = followers.drop('Unnamed: 0', axis=1)
        for i in range(followers['user_id'].min(), followers['user_id'].max()):
            key = "following:" + str(i)
            vals = followers.loc[followers['user_id'] == i]['follows_id'].tolist()
            self.con.sadd(key, *vals)

    # Get the timeline for a given user_id, broadcast would determine whether to fetch a premade timeline
    # or generate one on the fly
    def get_timeline(self, user_id, broadcast):
        # Just fetch the premade timeline
        if(broadcast == True):
            key = "timeline:" + str(user_id)
            timeline = self.con.zrange(key, 0, -1)
            print([x.decode('UTF-8') for x in timeline])
        else:
        # Union the sorted set of tweets for each person the given user is following to get the timeline
            union_key = "union:" + str(user_id)
            followees = self.get_followees(user_id)
            followees = ["posts_nb:" + s for s in followees]
            self.con.zunionstore(union_key, followees)
            timeline = self.con.zrange(union_key, 0, -1)
            print([x.decode('UTF-8') for x in timeline])

    # Get the set of followers for a given user
    def get_followers(self, user_id):
        key = "followers:" + str(user_id)
        followers = list(self.con.smembers(key))
        return ([x.decode('UTF-8') for x in followers])

    # Get the set of followees for a given user
    def get_followees(self, user_id):
        key = "following:" + str(user_id)
        followees = list(self.con.smembers(key))
        return ([x.decode('UTF-8') for x in followees])

    # Post all the tweets, broadcast determines whether a tweet is put on a followers timeline, or just
    # into a bucket of a tweets a user has posted
    def post_tweets(self,broadcast):
        tweets = pd.read_csv("C:/Users/bingb/Google Drive/DS4300/tweets.csv")
        tweets = tweets.drop('Unnamed: 0', axis=1)

        for i, row in tweets.iterrows():

            tweet = row['tweet_text']
            ts = time.time()
            user_id = row['user_id']

            followers = self.get_followers(user_id)

            if(broadcast==True):

                for user in followers:
                    timeline_key = "timeline:" + str(user)
                    self.con.zadd(timeline_key, tweet, ts)

            else:
                key_nb = "posts_nb:" + str(user_id)
                self.con.zadd(key_nb, tweet, ts)

    # Nuke the DB
    def clear_db(self):
        self.con.flushall()


