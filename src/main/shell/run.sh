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

# Cygwin conversion
if [[ "${CLASSPATH}" =~ cygdrive ]]; then
    CLASSPATH=$(echo "${CLASSPATH}" | sed -e 's#:#;#g;s#/cygdrive/c#C:#gi;s#/#\\#g')
fi
for ((i=1;i<10;i++)); do
    args[$i]=$(eval echo "\$$i")
    if [[ "${args[$i]}" =~ cygdrive ]]; then
        args[$i]=$(echo "${args[$i]}" | sed -e 's#:#;#g;s#/cygdrive/c#C:#gi;s#/#\\#g')
    fi
done

java $JAVA_OPTS -cp $CLASSPATH \
     com.anjlab.csv2db.Import \
     "${args[@]}"
