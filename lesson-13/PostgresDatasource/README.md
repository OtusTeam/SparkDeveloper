# Разработка собственного коннектора на Spark
Материалы для выполнения Домашнего задания.

Задание:
1. Склонируйте репозиторий https://github.com/OtusTeam/SparkDeveloper/tree/master/lesson-13/PostgresDatasource
2. Доработайте код в файле src/main/scala/ru/otus/spark/datasource/postgres/PostgresDatasource.scala:
    1. Реализовать чтение в несколько партиций
    2. Добавить новый параметр - размер партиции. Например, .option("partitionSize", "10")

Тест находится в файле src/test/scala/ru/otus/spark/PostgresqlSpec.scala

Проверка:
1. Тест:
    - sbt test
2. Проверка работы:
    1. Собираем uber jar:
        - sbt assembly
    2. Запускаем PostgreSQL:
        - docker run --rm --name postgres -p 5432:5432 -e POSTGRES_PASSWORD=password -d postgres
    3. Создаём таблицу:
        - docker exec -ti postgres psql -U postgres
        - CREATE TABLE users (user_id BIGINT PRIMARY KEY);
        - INSERT INTO users VALUES (1);
        - INSERT INTO users VALUES (2);
        - INSERT INTO users VALUES (3);
        - INSERT INTO users VALUES (4);
        - INSERT INTO users VALUES (5);
        - select * from users;
        - \q
    4. Читаем из источника:
        - spark-shell --driver-class-path target/scala-2.12/spark_custom_connector-assembly-0.1.jar --jars target/scala-2.12/spark_custom_connector-assembly-0.1.jar
        - val jdbcUrl = "jdbc:postgresql://localhost:5432/postgres"
        - val username = "postgres"
        - val password = "password"
        - val tableName = "users"
        - val df = spark.read.format("ru.otus.spark.datasource.postgres").option("url", jdbcUrl).option("user", username).option("password", password).option("tableName", tableName).load()
        - df.show
        - df.rdd.getNumPartitions


