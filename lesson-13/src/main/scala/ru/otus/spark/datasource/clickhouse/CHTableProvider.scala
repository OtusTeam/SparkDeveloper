package ru.otus.spark.datasource.clickhouse

import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util

/**
 * Единый интерфейс для всех источников в DataSourceV2
 */
class CHTableProvider extends TableProvider with DataSourceRegister {

  /**
   * Схема таблицы читается непосредственно из ClickHouse DESCRIBE TABLE
   * @param options
   * @return
   */
  def inferSchema(options: CaseInsensitiveStringMap): StructType = CHConnector.getSchema(options.get("tableName"))

  override def getTable(schema: StructType, partitioning: Array[Transform], properties: util.Map[String, String]): Table =
    new CHTable(schema, partitioning, properties)

  /**
   * короткое имя источника, которое можно использовать как
   * spark.read.format("clickhouse")...
   * @return
   */
  def shortName(): String = "clickhouse"

  /**
   * Можно определять схему вручную через
   * spark.read.format("clickhouse").schema(...)
   * @return
   */
  override def supportsExternalMetadata(): Boolean = true
}
