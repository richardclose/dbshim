package org.phasanix.dbshim

import java.sql.{PreparedStatement, ResultSet}
import java.time._
import java.sql.{Date => SqlDate}

/**
  * Helper methods to support Java 8 date and time API for
  * working with JDBC drivers that don't support JDBC 4.2 yet.
  */
trait Jsr310Support {

  def zoneId: ZoneId
  val driverImplementsJdbc42: Boolean

  // Get a DATE value as a java.time.LocalDate, called from generated code.
  def getLocalDate(rs: ResultSet, columnIndex: Int): LocalDate = {
    var d: LocalDate = if (driverImplementsJdbc42) {
      rs.getObject(columnIndex, classOf[LocalDate])
    } else {
      null
    }
    if (d == null)
      d = rs.getDate(columnIndex).toLocalDate
    d
  }

  //  Get a TIMESTAMP value, called from generated code. Makes the assumption
  //  that the date is in the system timezone.
  def getLocalDateTime(rs: ResultSet, columnIndex: Int): LocalDateTime = {
    var d: LocalDateTime = if (driverImplementsJdbc42) {
      rs.getObject(columnIndex, classOf[LocalDateTime])
    } else {
      null
    }
    if (d == null) {
      val date = rs.getDate(columnIndex)
      d = ZonedDateTime.ofInstant(Instant.ofEpochMilli(date.getTime), zoneId).toLocalDateTime
    }
    d
  }

  def setLocalDateTime(ps: PreparedStatement, parameterIndex: Int, value: LocalDateTime): Unit = {
    val instant = ZonedDateTime.of(value, zoneId).toInstant
    val date = new SqlDate(instant.toEpochMilli)
    ps.setDate(parameterIndex, date)
  }

  def setLocalDate(ps: PreparedStatement, parameterIndex: Int, value: LocalDate): Unit = {
    val instant = value.atStartOfDay(zoneId).toInstant
    val date = new SqlDate(instant.toEpochMilli)
    ps.setDate(parameterIndex, date)
  }

}
