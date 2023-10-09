package ru.otus.spark.datasource.clickhouse

import org.apache.spark.sql.connector.read.{Batch, Scan}
import org.apache.spark.sql.types.StructType

import java.util

/**
 * Логический план таблицы (logical plan)
 * @param schema
 * @param properties
 */
class CHScan(schema: StructType, properties: util.Map[String, String]) extends Scan {
  def readSchema(): StructType = schema

  /**
   * Название будет использоваться в логическом плане
   * @return
   */
  override def description(): String = "ch_scan"

  /**
   * Релизация чтения батчами, т.к. мы добавили BATCH_READ
   */
  override def toBatch: Batch = new CHBatch(schema, properties)
}
