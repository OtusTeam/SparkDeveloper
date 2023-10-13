# Использование Spark UI

## Запуск spark-shell в Докере
1. Запустить терминал в папке `lesson-21`
2. Запустить spark-shell в Docker:
   `docker run -it -p 4040:4040 --name spaontainer -v ./data/:/var/data apache/spark:latest /opt/spark/bin/spark-shell`

## Repartition
1. В `spark-shell` выполнить команду `:paste`
2. Вставить код:
```scala

    import org.apache.spark.sql.functions

    val customer_data = spark.read.format("json")
      .option("mode", "FAILFAST")
      .load("/var/data/customer_data.json")

    customer_data.rdd.getNumPartitions

    customer_data.repartition(31, col("Country"))
      .write.format("csv")
      .mode("OVERWRITE")
      .save("/var/data/repartitionedByKeyAndNum")

    customer_data.repartition(col("Country"))
      .write.format("csv")
      .mode("OVERWRITE")
      .option("maxRecordsPerFile", 200)
      .save("/var/data/repartitionedByKey")

```
## Cache
1. В `spark-shell` выполнить команду `:paste`
2. Вставить код:
```scala

    import org.apache.spark.sql.functions

    val customer_data = spark.read.format("json")
      .option("mode", "FAILFAST")
      .load("/var/data/customer_data.json")

    customer_data.cache
    customer_data.head(1)
```
3. Проверить кэш на `Spark UI`, вкладка `Storage`
4. Вставить код:
```scala

    val another_customer_data = spark.read.format("json")
      .option("mode", "FAILFAST")
      .load("/var/data/customer_data.json")

   val resDf = customer_data.groupBy("Country").count
   sc.setJobDescription("df from cache")
   resDf.show
```
5. Зайти на `Spark UI`, вкладка `SQL/DataFrame`. В даге проверить, что был использован кэш (операция `InMemoryTableScan)
