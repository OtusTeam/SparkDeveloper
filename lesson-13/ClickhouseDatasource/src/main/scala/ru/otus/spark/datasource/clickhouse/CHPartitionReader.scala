package ru.otus.spark.datasource.clickhouse

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder.Serializer
import org.apache.spark.sql.catalyst.encoders.{RowEncoder}
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader}
import org.apache.spark.sql.types.StructType

import java.sql.ResultSet

class CHPartitionReader(partition: InputPartition, schema: StructType) extends PartitionReader[InternalRow] {

  /**
   * SQL-запрос, который возвращает данные конкретной партиции
   */
  lazy val rs: ResultSet = CHConnector.connection.createStatement().executeQuery(
    partition.asInstanceOf[CHInputPartition].query)

  val serializer: Serializer[Row] = RowEncoder.apply(schema).createSerializer()

  def next(): Boolean = rs.next()

  /**
   * Преобразование строк из [[ResultSet]] в строки Spark ([[Row]]
   */
  def get(): InternalRow = serializer.apply(Row(rs.getString("col1"), rs.getInt("col2")))

  def close(): Unit = rs.close()
}
