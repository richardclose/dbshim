package org.phasanix.dbshim

import java.sql.ResultSet
import java.time.{LocalDate, LocalDateTime, ZoneId}

/**
  * Typeclass for reading values from a ResultSet.
  */
trait ReadRs[A] {
  def read(rs: ResultSet, index: Int): A
}

object ReadRs extends Jsr310Support {

  def zoneId: ZoneId = ZoneId.systemDefault()
  val driverImplementsJdbc42: Boolean = false

  implicit object readInt extends ReadRs[Int] {
    def read(rs: ResultSet, index: Int): Int = rs.getInt(index)
  }

  implicit object readLong extends ReadRs[Long] {
    def read(rs: ResultSet, index: Int): Long = rs.getLong(index)
  }

  implicit object readFloat extends ReadRs[Float] {
    def read(rs: ResultSet, index: Int): Float = rs.getFloat(index)
  }

  implicit object readDouble extends ReadRs[Double] {
    def read(rs: ResultSet, index: Int): Double = rs.getDouble(index)
  }

  implicit object readString extends ReadRs[String] {
    def read(rs: ResultSet, index: Int): String = rs.getString(index)
  }

  implicit object readDate extends ReadRs[java.util.Date] {
    def read(rs: ResultSet, index: Int): java.util.Date = rs.getDate(index)
  }

  implicit object readBoolean extends ReadRs[Boolean] {
    def read(rs: ResultSet, index: Int): Boolean = rs.getBoolean(index)
  }

  implicit object readLocalDate extends ReadRs[LocalDate] {
    def read(rs: ResultSet, index: Int): LocalDate = getLocalDate(rs, index)
  }

  implicit object readLocalDateTime extends ReadRs[LocalDateTime] {
    def read(rs: ResultSet, index: Int): LocalDateTime = getLocalDateTime(rs, index)
  }

}
