import redis

r = redis.StrictRedis(host='localhost', port=6379, db=0)
keys = r.keys ("tweet:*")

for key in keys:
    print key.split(':')[1], r.get(key)

print 'Tweets Read:', len(keys)