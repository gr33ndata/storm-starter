#!/bin/bash
# Helper script to check if data was correcly retrieved
ntweets=$(redis-cli keys "tweet:*" | egrep -v 'ERR|empty|^$' | wc -l)
nlangs=$(redis-cli keys "lang:*" | egrep -v 'ERR|empty|^$' | wc -l)
sum=0
for lang in $(redis-cli keys "lang:*" | sed 's/.*"//g')
do
  sum=$((sum + `redis-cli get $lang | sed 's/"//g'`))
done
echo "There are $ntweets tweets and $nlangs languages on the Redis database, and the sum of all languages is $sum."
