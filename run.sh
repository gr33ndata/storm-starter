#!/bin/bash
# To run the old topology
#mvn -e -f m2-pom.xml compile exec:java -Dexec.classpathScope=compile -Dstorm.topology=storm.starter.TweetsLanguages
# To run the new topology, with paragraphs to sentences splitting
mvn -e -f m2-pom.xml compile exec:java -Dexec.classpathScope=compile -Dstorm.topology=storm.starter.TweetsLanguagesSplit
