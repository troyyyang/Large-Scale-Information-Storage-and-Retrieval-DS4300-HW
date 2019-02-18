from HW2tyang import RedisAPI

import redis

r = redis.Redis(host='localhost', port=6379, db = 0)
api = RedisAPI(r)
api.clear_db()
api.add_followers()
api.add_following()
api.post_tweets(True)
api.post_tweets(False)
api.get_timeline(1234, True)
api.get_timeline(1234, False)

