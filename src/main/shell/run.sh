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

java -cp $CLASSPATH \
      \
     com.anjlab.csv2db.Import \
     "$1" "$2" "$3" "$4" "$5" "$6" "$7" "$8" "$9"