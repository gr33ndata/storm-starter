# Storm

This is a fork of [storm-starter](http://github.com/nathanmarz/storm-starter) that adds a new topology called TweetsLanguages. This topology fetches tweets of a certain topic on Twitter and classifies their languages using [Langid.py](https://github.com/saffsd/langid.py). Then, the tweets are stored in a Redis database, whose key is the tweet id and the value is the language. There is a bolt that counts the number of tweets of each language, and also stores them in the Redis database. See the topology below:

![Topology](https://raw.github.com/gr33ndata/storm-starter/langidysl/docs/topology.png "Topology")

In order to run the topology, you'll need Redis and Langid.py, besides the dependencies of the original storm-starter.

First, copy the file `config.yml.example` to `config.yml` and set the configurations. Pay attention to the "mode" option. If it's "local", the topology will run in "local" mode, if it's "cluster", the topology will run on a cluster.

Then, simply run (once on repository root):

`$ mvn -f m2-pom.xml compile exec:java -Dexec.classpathScope=compile -Dstorm.topology=storm.starter.TweetsLanguages`

Or:

`./run.sh`

After that, you can run the Shell script `redis-count.sh`, which outputs information about the keys stored in your Redis database. The sum of the counts of each language should be the same as the total number of tweets.
