spark-submit \
    --master spark://localhost:7077 \
    --deploy-mode client \
    --py-files /Users/jakubpluta/Repositories/priv/streamer/sparky.zip \
    --conf "spark.executorEnv.PYTHONPATH=/Users/jakubpluta/Repositories/priv/streamer/sparky.zip" \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4 \
    /Users/jakubpluta/Repositories/priv/streamer/sparky/app.py