from pyspark.sql.session import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType
from pyspark.sql.functions import col
import logging
from pyspark.sql.dataframe import DataFrame

def create_logger() -> logging.Logger:
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    return logging.getLogger(__name__)

def create_spark_session()-> SparkSession:
    return (SparkSession
        .builder
        .appName("Pyspark yellow taxi")
        .master("local[*]")
        .getOrCreate()
            )


def get_taxi_data(spark_session: SparkSession, taxi_data_path: str) -> DataFrame:
    schema = StructType([
        StructField("VendorID", IntegerType()),
        StructField("tpep_pickup_datetime", TimestampType()),
        StructField("tpep_dropoff_datetime", TimestampType()),
        StructField("passenger_count", IntegerType()),
        StructField("trip_distance", DoubleType()),
        StructField("RatecodeID", IntegerType()),
        StructField("store_and_fwd_flag", StringType()),
        StructField("PULocationID", IntegerType()),
        StructField("DOLocationID", IntegerType()),
        StructField("payment_type", IntegerType()),
        StructField("fare_amount", DoubleType()),
        StructField("extra", DoubleType()),
        StructField("mta_tax", DoubleType()),
        StructField("tip_amount", DoubleType()),
        StructField("tolls_amount", DoubleType()),
        StructField("improvement_surcharge", DoubleType()),
        StructField("total_amount", DoubleType()),
    ])
    return spark_session.read.schema(schema).parquet(taxi_data_path)

def get_zone_data(spark_session: SparkSession, zone_data_path: str) -> DataFrame:
    schema = StructType([
        StructField("LocationID", IntegerType()),
        StructField("Borough", StringType()),
        StructField("Zone", StringType()),
        StructField("service_zone", StringType())
    ])
    return spark_session.read.schema(schema).csv(zone_data_path, header=True)




if __name__ == "__main__":

    log = create_logger()
    spark = create_spark_session()
    base_df = get_taxi_data(spark_session=spark, taxi_data_path="resources/data/yellow_taxi_jan_25_2018")
    base_df.cache()
    zone_df = get_zone_data(spark_session=spark, zone_data_path="resources/data/taxi_zones.csv")

    log.info("task1 started")
    task1 = (base_df.join(zone_df, col("DOLocationID") == col("LocationID"))
                     .groupBy("Zone").count().withColumnRenamed("count", "trips_number")
                     .orderBy("trips_number", ascending=False)
    )
    task1.show(n=10, truncate=False)
    task1.write.mode("overwrite").parquet("result/task1/task1.parquet")
    log.info("task1 finished")

    log.info("task2 started")
    task2 = (base_df.groupBy("tpep_pickup_datetime").count().withColumnRenamed("count", "trips_number")
                   .orderBy("trips_number", ascending=False)
                   .select(col("tpep_pickup_datetime"),
                           col("trips_number"))
             )
    task2.show(n=10, truncate=False)
    # собираем список строк для записи в файл
    task2_text = task2.rdd.map(lambda row: f"{row.tpep_pickup_datetime},{row.trips_number}\n").collect()
    with open("result/task2/task2.txt", "w") as f:
        f.writelines(task2_text)
    log.info("task2 finished")

    log.info("task3 started")
    # в pyspark нет dataset, поэтому дя тренировки выполнено в df
    task3 = (base_df.groupBy("trip_distance").count().withColumnRenamed("count", "trips_number")
                   .select(col("trip_distance"),
                           col("trips_number"))
                    .orderBy("trips_number", ascending=False)
             )
    task3.show(n=10, truncate=False)
    task3.write.mode("overwrite").parquet("result/task3/task3.parquet")
    log.info("task3 finished")
    base_df.unpersist()
    spark.stop()
    log.info("Spark session stopped. application finished")