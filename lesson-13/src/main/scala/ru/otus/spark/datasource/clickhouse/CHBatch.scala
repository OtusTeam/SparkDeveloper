package ru.otus.spark.datasource.clickhouse

import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory}
import org.apache.spark.sql.types.StructType

import java.util

/**
 * Физический план чтения таблицы.
 * @param schema
 * @param properties
 */
class CHBatch(schema: StructType, properties: util.Map[String, String] ) extends Batch {

  /**
   * Разделение одного источника данных (таблицы) на несколько партиций. Каждая партиция будет читаться отдельно одним executor
   * @return
   */
  def planInputPartitions(): Array[InputPartition] = {
    //Берем партиции из метаданных CH
    val parts = CHConnector.getPartitions(properties.get("tableName"))
    //Указываем хост партиции
    val hostName = CHConnector.getHostName
    //Указываем SQL-запрос, по которому можно получить данные партиции
    val query = s"SELECT * FROM ${properties.get("tableName")} WHERE ${properties.get("partitionKey")} = "
    parts.map(p => new CHInputPartition(schema, s"$query $p", hostName)).toArray
  }


  def createReaderFactory(): PartitionReaderFactory = new CHPartitionReaderFactory(schema)
}
