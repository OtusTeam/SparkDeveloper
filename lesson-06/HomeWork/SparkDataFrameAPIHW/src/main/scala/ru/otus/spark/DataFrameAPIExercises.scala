package ru.otus.spark

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{ArrayType, IntegerType, StringType, StructField, StructType}


object DataFrameAPIExercises extends SparkSessionWrapper {

  /**
   * Все задачи можно (и нужно) выполнить без UDF
   */

  /**
   * Даны студенты со списком компетенций.
   * Задача: вернуть датафрейм, где будут только те студенты, которые знают Python
   */
  def ex1(): Unit = {

    // Данные
    val data = Seq((1, "Alice", Seq("Java", "Python", "SQL")),
      (2, "Bob", Seq("C++", "R")),
      (3, "Charlie", Seq("Python", "JavaScript")),
      (4, "David", Seq("Java", "C#", "Python")))

    val studentData = spark.createDataFrame(data).toDF("student_id", "student_name", "skills")

    // НАПИШИТЕ СВОЕ РЕШЕНИЕ ЗДЕСЬ
    val resDf = studentData
    resDf.printSchema()
    resDf.show

  }

  /**
   * Даны заказы со списком купленных продуктов.
   * Задача: составить рейтинг самых популярных продуктов.
   * То есть, найти количество проданных единиц по каждому продукту и отсортировать датафрейм по этому значению.
   * В результате датафрейм должен иметь три столбца: ID продукта, название продукта и количество купленных единиц.
   */
  def ex2(): Unit = {

    //Данные
    val data = Seq(
      Row("1", "2023-01-01", Seq(Row("101", "Product A", 2), Row("102", "Product B", 3))),
      Row("2", "2023-01-02", Seq(Row("103", "Product C", 1))),
      Row("3", "2023-01-03", Seq(Row("101", "Product A", 1), Row("104", "Product D", 4)))
    )

    //Схема
    val schema = StructType(Seq(StructField("order_id", StringType, nullable = false),
      StructField("order_date", StringType, nullable = false),
      StructField("order_items", ArrayType(
        StructType(Seq(StructField("product_id", StringType, nullable = false),
          StructField("product_name", StringType, nullable = true),
          StructField("quantity", IntegerType, nullable = true))))
        , nullable = true)
    ))

    val salesData = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

    // НАПИШИТЕ СВОЕ РЕШЕНИЕ ЗДЕСЬ
    val resDf = salesData

    resDf.printSchema()
    resDf.show(false)

  }

  /**
   * Дан список событий.
   * Задача: добавить в датафрейм столбец, содержащий день недели (ПН, ВТ, СР), в который произошло событие.
   *
   */
  def ex3(): Unit = {

    // Данные
    val data = Seq((1, "Event 1", "2023-01-10 08:00:00"),
      (2, "Event 2", "2023-01-15 14:30:00"),
      (3, "Event 3", "2023-01-20 19:45:00"))

    val eventData = spark.createDataFrame(data).toDF("event_id", "event_name", "event_date")

    // НАПИШИТЕ СВОЕ РЕШЕНИЕ ЗДЕСЬ
    val resDf = eventData
    resDf.printSchema
    resDf.show

  }

  /**
   * Даны покупки с полями:
   *   1. ID транзакции
   *   2. Дата покупки
   *   3. Информация о клиенте в формате JSON
   *   4. Информация о купленных товарах в формате JSON
   * Задача:
   *   - распарсить данные о клиенте и купленных товарах из JSON в отдельные столбцы DataFrame. Все поля из JSON
   *     записать в отдельные подстолбцы столбцов customer_info и order_items соответственно
   *   - По каждому клиенту посчитать общую сумму всех покупок.
   */

  def ex4(): Unit = {

    // Данные
    val data = Seq(
      (1, "2023-01-10",
        "{\n  \"customer_id\": 101,\n  \"customer_name\": \"Alice\",\n  \"customer_contact_info\": {\n    \"e-mail\": \"alice@example.com\",\n    \"phone\": \"123-456-7890\"\n  }\n}",
        "{\n  \"items\": [\n    {\n      \"id\": 1,\n      \"name\": \"Product A\",\n      \"quantity\": 2,\n      \"price\": 10.0\n    },\n    {\n      \"id\": 2,\n      \"name\": \"Product B\",\n      \"quantity\": 1,\n      \"price\": 20.0\n    }\n  ]\n}"),
      (2, "2023-01-12",
        "{\n  \"customer_id\": 102,\n  \"customer_name\": \"Bob\",\n  \"customer_contact_info\": {\n    \"e-mail\": \"bob@example.com\",\n    \"phone\": \"987-654-3210\"\n  }\n}",
        "{\n  \"items\": [\n    {\n      \"id\": 1,\n      \"name\": \"Product A\",\n      \"quantity\": 3,\n      \"price\": 10.0\n    },\n    {\n      \"id\": 3,\n      \"name\": \"Product C\",\n      \"quantity\": 2,\n      \"price\": 15.0\n    }\n  ]\n}"),
      (3, "2023-01-15",
        "{\n  \"customer_id\": 101,\n  \"customer_name\": \"Alice\",\n  \"customer_contact_info\": {\n    \"e-mail\": \"alice@example.com\",\n    \"phone\": \"123-456-7890\"\n  }\n}",
        "{\n  \"items\": [\n    {\n      \"id\": 2,\n      \"name\": \"Product B\",\n      \"quantity\": 2,\n      \"price\": 20.0\n    },\n    {\n      \"id\": 3,\n      \"name\": \"Product C\",\n      \"quantity\": 1,\n      \"price\": 15.0\n    }\n  ]\n}")
    )

    val orderData = spark.createDataFrame(data).toDF("order_id", "order_date", "customer_info", "order_items")

    // НАПИШИТЕ СВОЕ РЕШЕНИЕ ЗДЕСЬ
    val resDf = orderData
    resDf.printSchema()
    resDf.show()

  }

}
