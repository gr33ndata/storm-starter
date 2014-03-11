#!/bin/bash
# Helper script to check if data was correcly retrieved
n=$(redis-cli keys "*" | egrep -v 'ERR|empty|^$' | wc -l)
nlangs=$(redis-cli keys "lang:*" | egrep -v 'ERR|empty|^$' | wc -l)
ntweets=$((n - nlangs))
sum=0
for lang in $(redis-cli keys "lang:*" | sed 's/.*"//g')
do
  sum=$((sum + `redis-cli get $lang | sed 's/"//g'`))
done
echo "There are $n records on the Redis database, from which $nlangs are languages and $ntweets are tweets, and the sum of all languages is $sum."
