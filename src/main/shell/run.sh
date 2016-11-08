#!/bin/bash
CLASSPATH=

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

for i in $DIR/lib/*.jar
do
    CLASSPATH=$i:$CLASSPATH
done

for i in $DIR/*.jar
do
    CLASSPATH=$i:$CLASSPATH
done

java $JAVA_OPTS -cp $CLASSPATH \
     com.anjlab.csv2db.Import \
     "$@"
