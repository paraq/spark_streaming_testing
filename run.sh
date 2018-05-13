$SPARK_HOME/bin/spark-submit --class paraq.Twitterstats --master local[2] target/scala-2.11/Twitterstats-assembly-0.1-SNAPSHOT.jar  $1 $2 $3
