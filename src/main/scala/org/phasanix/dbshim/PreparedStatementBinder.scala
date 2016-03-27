package org.phasanix.dbshim

import java.sql.{PreparedStatement, ResultSet}

/**
  * Fluent API for binding params to a prepared statement
  */
class PreparedStatementBinder(ps: PreparedStatement) {

  private var currentIndex: Int = 1

  /** Bind the given value to the next parameter of the prepared statement */
  def set[A: BindPsParam](value: A): PreparedStatementBinder = {
    implicitly[BindPsParam[A]].bind(ps, currentIndex, value)
    currentIndex += 1
    this
  }

  /** execute the wrapped PreparedStatement as a query */
  def query(): ResultSet = ps.executeQuery()

  /** execute the wrapped PreparedStatement as an update */
  def update(): Int = ps.executeUpdate()

}
