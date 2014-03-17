#!/bin/bash
mvn -e -f m2-pom.xml compile exec:java -Dexec.classpathScope=compile -Dstorm.topology=storm.starter.TweetsLanguages
