package ru.otus.spark.datasource.clickhouse

import org.apache.spark.sql.connector.read.{Scan, ScanBuilder}
import org.apache.spark.sql.types.StructType

import java.util

/**
 * Здесь можно реализовать оптимизацию PushDown [[org.apache.spark.sql.connector.read.SupportsPushDownFilters]]
 * @param schema
 * @param properties
 */
class CHScanBuilder(schema: StructType, properties: util.Map[String, String]) extends ScanBuilder {
  def build(): Scan = new CHScan(schema, properties)
}
