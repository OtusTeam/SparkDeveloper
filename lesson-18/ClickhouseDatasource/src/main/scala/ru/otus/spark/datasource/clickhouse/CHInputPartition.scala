package ru.otus.spark.datasource.clickhouse

import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.types.StructType

/**
 * Представление конкретной партиции в ClickHouse.
 * Query - SQL-запрос, которые вернет конкретную партицию
 * Например, Select * from
 */
class CHInputPartition(val schema: StructType, val query: String, shardHost: String) extends InputPartition {

  /**
   * Указываем, что чтение партиции нужно делать на одной и той же ноде
   * @return
   */
  override def preferredLocations(): Array[String] = Array(shardHost)


}
