package org.phasanix.dbshim

import java.sql.{PreparedStatement, ResultSet, Timestamp, Date => SqlDate}
import java.time._

/**
  * Helper methods to support Java 8 date and time API for
  * working with JDBC drivers that don't support JDBC 4.2 yet.
  */
trait Jsr310Support {

  def zoneId: ZoneId
  val driverImplementsJdbc42: Boolean

  // Get a DATE value as a java.time.LocalDate, called from generated code.
  def getLocalDate(rs: ResultSet, columnIndex: Int): LocalDate = {

    if (driverImplementsJdbc42) {
      rs.getObject(columnIndex, classOf[LocalDate])
    } else {
      val d1 = rs.getDate(columnIndex)
      if (d1 == null)
        LocalDate.MIN // was null, will be ignored
      else
        d1.toLocalDate
    }

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
      val ts = rs.getTimestamp(columnIndex)
      d = ZonedDateTime.ofInstant(Instant.ofEpochMilli(ts.getTime), zoneId).toLocalDateTime
    }
    d
  }

  def setLocalDateTime(ps: PreparedStatement, parameterIndex: Int, value: LocalDateTime): Unit = {
    val instant = ZonedDateTime.of(value, zoneId).toInstant
    val ts = new Timestamp(instant.toEpochMilli)
    ps.setTimestamp(parameterIndex, ts)
  }

  def setLocalDate(ps: PreparedStatement, parameterIndex: Int, value: LocalDate): Unit = {
    val instant = value.atStartOfDay(zoneId).toInstant
    val date = new SqlDate(instant.toEpochMilli)
    ps.setDate(parameterIndex, date)
  }

}

object Jsr310SupportSystem extends Jsr310Support {
  def zoneId: ZoneId = ZoneId.systemDefault()
  val driverImplementsJdbc42: Boolean = false
}