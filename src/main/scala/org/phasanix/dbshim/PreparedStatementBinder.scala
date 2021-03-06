package org.phasanix.dbshim

import java.sql.{PreparedStatement, ResultSet}

/**
  * Fluent API for binding params to a prepared statement
  */
class PreparedStatementBinder(val ps: PreparedStatement) {

  private var currentIndex: Int = 1

  /**
    * Reset position index
    * @return self
    */
  def reset(): PreparedStatementBinder = {
    currentIndex = 1
    this
  }

  /** Set the given value to the next parameter of the prepared statement */
  def set[A: BindPsParam](value: A): PreparedStatementBinder = {
    implicitly[BindPsParam[A]].bind(ps, currentIndex, value)
    currentIndex += 1
    this
  }

  /** Set the given value to the next parameter for an Option
    * (Can't use set[A] -- different kind) */
  def setOpt[A: BindPsParam : scala.reflect.runtime.universe.TypeTag](maybeValue: Option[A]): PreparedStatementBinder = {
    maybeValue match {
      case Some(value) => implicitly[BindPsParam[A]].bind(ps, currentIndex, value)
      case None => ps.setNull(currentIndex, Db.sqlTypeOf[A])
    }
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

  /** execute as query and return sequence of parsed objects */
  def toSeq[A](func: ResultSet => A): Seq[A] = {
    val rs = ps.executeQuery()
    Db.resultSetVector(rs)(func)
  }

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

  /**
    * Execute the wrapped PreparedStatement as a query
    * @param fn conversion from ResultSet to A
    * @tparam A target type
    * @return sequence of A
    */
  def parse[A](fn: ResultSet => A): Seq[A] = {
    Db.resultSetVector(ps.executeQuery())(fn)
  }

  /**
    * Execute the wrapped PreparedStatement as a query
    * @param fn conversion from ResultSet to A
    * @tparam A target type
    * @return None if no rows selected, else Some(first row)
    *         if one or more rows selected.
    */
  def parseOpt[A](fn: ResultSet => A): Option[A] = {
    Db.resultSetOpt(ps.executeQuery())(fn)
  }

}
