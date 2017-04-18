package org.phasanix.dbshim

import java.sql.{PreparedStatement, ResultSet}

/**
  * Fluent API for binding params to a prepared statement
  */
class PreparedStatementBinder(ps: PreparedStatement) {

  private var currentIndex: Int = 1

  /** Set the given value to the next parameter of the prepared statement */
  def set[A: BindPsParam](value: A): PreparedStatementBinder = {
    implicitly[BindPsParam[A]].bind(ps, currentIndex, value)
    currentIndex += 1
    this
  }

  /** bind */
  def bind[A](binder: JdbcBinder[A], value: A, skipList: Int*): PreparedStatementBinder = {
    if (skipList.isEmpty)
      binder.bindPreparedStatement(ps, value)
    else
      binder.bindPreparedStatementPartial(ps, value, skipList: _*)
    this
  }

  /** bind for update */
  def bindForUpdate[A](binder: JdbcBinder[A], value: A, keyOffsets: Int*): PreparedStatementBinder = {
    assert(keyOffsets.nonEmpty)
    binder.bindPreparedStatementForUpdate(ps, value, keyOffsets: _*)
    this
  }

  /** execute the wrapped PreparedStatement as a query */
  def query(): ResultSet = ps.executeQuery()

  /** execute the wrapped PreparedStatement as an update */
  def update(): Int = ps.executeUpdate()

  /**
    * execute the wrapped PreparedStatement as on insert.
    *
    * If successful and auto-increment key of type A generated,
    * return Some(key value)
    */
  def insert[A: ReadRs](): Option[A] = {
    val count = ps.executeUpdate()
    Db.resultSetScalar[A](ps.getGeneratedKeys)
  }

}
