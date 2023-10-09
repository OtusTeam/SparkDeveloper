package ru.otus.spark.datasource.clickhouse

import org.apache.spark.sql.connector.catalog.{SupportsRead, TableCapability}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util

/** Логическое представление источника. Может быть файлом, папкой, таблицей в базе данных, потоком в Kafka.
  * Может использоваться для чтения и/или записи ([[SupportsRead]], [[SupportsWrite]])
  * @param schema
  * @param partitioning физические партиции данных в формате физического плана Catalyst (например, из Spark-каталога).
  *        По умолчанию партиции инферируются.
  * @param properties
  */
class CHTable(
    schema: StructType,
    partitioning: Array[Transform],
    properties: util.Map[String, String]
) extends SupportsRead {

  /** Логический план таблицы
    * @param options
    * @return
    */
  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
    new CHScanBuilder(schema, options)

  def name(): String = properties.get("tableName")

  def schema(): StructType = schema

  /** Как можно читать таблицу (streaming, batch, micro-batch)
    */
  override def capabilities(): util.Set[TableCapability] =
    util.Set.of(TableCapability.BATCH_READ)
}
