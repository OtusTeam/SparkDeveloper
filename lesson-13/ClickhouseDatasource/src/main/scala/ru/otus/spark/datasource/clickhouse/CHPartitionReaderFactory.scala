package ru.otus.spark.datasource.clickhouse

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.types.StructType

/**
 * Фабрика объектов чтения таблицы
 * @param schema
 */
class CHPartitionReaderFactory(schema: StructType) extends PartitionReaderFactory {
  def createReader(partition: InputPartition): PartitionReader[InternalRow] = new CHPartitionReader(partition, schema)
}
