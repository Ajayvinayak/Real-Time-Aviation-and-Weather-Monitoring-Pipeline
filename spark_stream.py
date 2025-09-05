from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import (StructType, StructField, StringType, 
                               IntegerType, DoubleType, FloatType)

def main():
    spark = SparkSession.builder \
        .appName("RealTimeFlightMonitor") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")

    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:2092") \
        .option("subscribe", "flight-delays") \
        .load()

    # The final, complete schema that matches the producer
    schema = StructType([
        StructField("flight_iata", StringType()),
        StructField("airline_name", StringType()),
        StructField("departure_airport", StringType()),
        StructField("arrival_airport", StringType()),
        StructField("status", StringType()),
        StructField("latitude", DoubleType()),
        StructField("longitude", DoubleType()),
        StructField("altitude", DoubleType()),
        StructField("speed_kph", IntegerType()),
        StructField("weather_temp_c", FloatType()),
        StructField("weather_wind_kph", FloatType()),
        StructField("dep_scheduled", StringType()),
        StructField("dep_estimated", StringType()),
        StructField("arr_scheduled", StringType()),
        StructField("arr_estimated", StringType()),
        StructField("disaster_alert", StringType())
    ])

    parsed_df = kafka_df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

    query = parsed_df.writeStream \
        .foreachBatch(lambda batch_df, _: batch_df.write \
                      .format("jdbc") \
                      .option("url", "jdbc:mysql://mysql:3306/airline_db") \
                      .option("driver", "com.mysql.cj.jdbc.Driver") \
                      .option("dbtable", "live_flight_data") \
                      .option("user", "root") \
                      .option("password", "Ajay$123") \
                      .mode("append") \
                      .save()
                     ) \
        .outputMode("update") \
        .start()

    print("Real-time monitoring pipeline started. Waiting for data from Kafka...")
    query.awaitTermination()

if __name__ == "__main__":
    main()