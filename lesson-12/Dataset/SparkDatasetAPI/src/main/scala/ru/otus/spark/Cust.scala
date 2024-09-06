package ru.otus.spark

import java.sql.Date
import java.text.SimpleDateFormat
import java.util

case class Cust(
    CustomerID: Long,
    Name: String,
    Birthdate: Date,
    Country: String
)

object Cust {
  private val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  def apply(c: Customers): Cust = {
    val utilDate: util.Date = format.parse(c.Birthdate)
    val sqlDate: Date       = new Date(utilDate.getTime)
    Cust(
      c.CustomerID.toLong,
      c.Name,
      sqlDate,
      c.Country
    )
  }
}
