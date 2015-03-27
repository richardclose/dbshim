/*

The MIT License (MIT)

Copyright (c) 2015 Richard Close

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

*/

package org.phasanix.dbshim

import java.sql.{PreparedStatement, Connection, ResultSet}

import reflect.runtime.universe._

/**
 * Utility methods for working with JDBC classes.
 */

object Db {

  /**
   * Iterator[ResultSet] which calls a function at end to close resources.
   *
   * This implementation addresses the mismatch between the semantics of Iterator
   * and ResultSet (ResultSet.next() moves the cursor and indicates end in a
   * single operation, whereas they are separate in Iterator).
   * @param rs ResultSet to iterate
   * @param onClose function to call at the end of the ResultSet.
   */
  class CloseableResultSetIterator(rs: ResultSet, onClose: () => Unit) extends Iterator[ResultSet] {

    private[this] var _readPending = false
    private[this] var _hasNext = false
    private def _advance(): Unit = {
      _hasNext = rs.next()
      if (!_hasNext)
        close()
    }

    def hasNext: Boolean = {
      if (!_readPending) {
        _readPending = true
        _advance()
      }
      _hasNext
    }

    def next(): ResultSet = {
      if (_readPending) {
        _readPending = false
      } else {
        _advance()
      }
      rs
    }

    def close(): Unit = {
      rs.close()
      onClose()
    }
  }

  private class Closer(toClose: AutoCloseable*) extends Function0[Unit] {
    def apply(): Unit = toClose.foreach(_.close())
  }

  val Noop: () => Unit = { () => () }

  /**
   * Create an iterator for the given ResultSet.
   */
  def asIterator(rs: ResultSet): Iterator[ResultSet] = new CloseableResultSetIterator(rs, onClose = Noop)

  /**
   * Invoke the given PreparedStatement, then close it.
   * @param stmt prepared statement to invoke
   * @param block block to execute.
   * @tparam A result type
   * @return result
   */
  def withResultSet[A](stmt: PreparedStatement)(block: ResultSet => A): A = {
    try {
      block(stmt.executeQuery())
    } finally {
      stmt.close()
    }
  }

  /**
   * Prepare and execute the given query, binding the arguments and passing the resultset
   * to the given block.
   * @param sql sql query
   * @param args arguments to bind
   * @param block block to execute
   * @param conn database connection
   * @tparam A result type
   * @return result
   */
  def withResultSet[A](sql: String, args: AnyRef*)(block: ResultSet => A)(implicit conn: Connection): A = {
    val stmt = conn.prepareStatement(sql)
    args.zipWithIndex.foreach { case (arg, i) => stmt.setObject(i, arg) }

    try {
      block(stmt.executeQuery())
    } finally {
      stmt.close()
    }
  }


  /**
   * Prepare and execute the given query, binding the arguments, and returning an auto-closing
   * iterator for the resultset. The <code>Connection</code> is also closed, on the assumption
   * that it belongs to a connection pool.
   * @param sql sql query
   * @param args arguments to bind
   * @param conn database connection
   * @return resultset iterator
   */
  def autocloseQuery(sql: String, args: AnyRef*)(implicit conn: Connection): Iterator[ResultSet] = {
    val stmt = conn.prepareStatement(sql)
    args.zipWithIndex.foreach { case (arg, i) => stmt.setObject(i, arg) }

    val rs = stmt.executeQuery()
    new CloseableResultSetIterator(rs, new Closer(stmt, conn))
  }

  /** Runtime type to sql type */
  def sqlTypeOf[A : TypeTag]: Int = sqlTypeOf(typeOf[A])

  /** Runtime type to sql type */
  def sqlTypeOf(tpe: Type): Int = {
    import java.sql.{Types => T}

    tpe match {
      case x if x =:= typeOf[Int] => T.INTEGER
      case x if x =:= typeOf[Long] => T.BIGINT
      case x if x =:= typeOf[Float] => T.FLOAT
      case x if x =:= typeOf[Double] => T.DOUBLE
      case x if x =:= typeOf[java.util.Date] => T.TIMESTAMP
      case x if x =:= typeOf[String] => T.VARCHAR
      case x if x =:= typeOf[Boolean] => T.BOOLEAN

      case _ => T.JAVA_OBJECT
    }
  }


}
