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
    *
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
    *
    * @param stmt prepared statement to invoke
   * @param block block to execute.
   * @tparam A result type
   * @return result
   */
  def withResultSet[A](stmt: PreparedStatement)(block: ResultSet => A): A = {
    try {
      block(stmt.executeQuery())
    } finally {
      stmt.close() // Should close resultset too
    }
  }

  /**
   * Prepare and execute the given query, binding the arguments and passing the resultset
   * to the given block.
    *
    * @param sql sql query
   * @param args arguments to bind
   * @param block block to execute
   * @param conn database connection
   * @tparam A result type
   * @return result
   */
  @deprecated("Use prepare/set/query/RichResultSet or PreparedStatementBinder.parse")
  def withResultSet[A](sql: String, args: Any*)(block: ResultSet => A)(implicit conn: Connection): A = {
    val stmt = conn.prepareStatement(sql)
    args.zipWithIndex.foreach { case (arg, i) => stmt.setObject(i + 1, arg.asInstanceOf[AnyRef]) }

    try {
      block(stmt.executeQuery())
    } finally {
      stmt.close()
    }
  }

  /**
    * Prepare and execute the given query, binding the arguments and passing the resultset
    * to the given block once for the first result, or None if empty.
    *
    * @param sql sql query
    * @param args arguments to bind
    * @param block block to execute
    * @param conn database connection
    * @tparam A result type
    * @return result option
    */
  @deprecated("Use prepare/set/query/RichResultSet or PreparedStatementBinder.parseOpt")
  def withResultSetOpt[A](sql: String, args: Any*)(block: ResultSet => A)(implicit conn: Connection): Option[A] = {
    withResultSet(sql, args: _*) { rs =>
      if (rs.next())
        Some(block(rs))
      else
        None
    }
  }

  /**
    * Execute the given prepared statement as query, pass the resultset to the
    * given block once for the first result, or None if empty
    * @param ps prepared statement
    * @param block block to execute
    * @param conn database connection
    * @tparam A result type
    * @return result option
    */
  def withResultSetOpt[A](ps: PreparedStatement)(block: ResultSet => A)(implicit conn: Connection): Option[A] = {
    withResultSet(ps) { rs =>
      if (rs.next())
        Some(block(rs))
      else
        None
    }
  }

  /**
   * Prepare and execute the given query, binding the arguments, and returning an auto-closing
   * iterator for the resultset. The <code>Connection</code> is also closed, on the assumption
   * that it belongs to a connection pool.
    *
    * @param sql sql query
   * @param args arguments to bind
   * @param conn database connection
   * @return resultset iterator
   */
  def autocloseQuery(sql: String, args: Any*)(implicit conn: Connection): Iterator[ResultSet] = {
    val stmt = conn.prepareStatement(sql)
    args.zipWithIndex.foreach { case (arg, i) => stmt.setObject(i+1, arg.asInstanceOf[AnyRef]) }

    val rs = stmt.executeQuery()
    new CloseableResultSetIterator(rs, new Closer(stmt, conn))
  }

  /**
    * Scalar result from a ResultSet, i.e. the value in the first column
    * of the first row.
    *
    * @param rs ResultSet
    * @tparam A required return type
    */
  def resultSetScalar[A : ReadRs](rs: ResultSet): Option[A] = {
    if (rs.next()) {
      Some(implicitly[ReadRs[A]].read(rs, 1))
    } else {
      None
    }
  }

  /**
    * Converted row from a ResultSet, or None if the ResultSet is
    * empty.
    */
  def resultSetOpt[A](rs: ResultSet)(fn: ResultSet => A): Option[A] = {
    if (rs.next())
      Some(fn(rs))
    else
      None
  }

  /**
    * Evaluate given resultset, giving sequence of required type
    * @param rs resultset
    * @param fn function that creates instances of required type from resultset
    * @tparam A required type
    * @return non-lazy sequence of required type.
    */
  def resultSetVector[A](rs: ResultSet)(fn: ResultSet => A): Seq[A] = {
    val ret = collection.mutable.ArrayBuffer.empty[A]
    try {
      while (rs.next())
        ret.append(fn(rs))
    } finally {
      rs.close()
    }
    ret
  }

  /**
    * Create PreparedStatement for given sql, wrapped in PreparedStatementBinder
    */
  def prepare(sql: String)(implicit conn: Connection): PreparedStatementBinder = {
    new PreparedStatementBinder(conn.prepareStatement(sql))
  }

  /**
    * Create PreparedStatement for given sql, wrapped in PreparedStatementBinder
    */
  def prepareInsert(sql: String)(implicit conn: Connection): PreparedStatementBinder = {
    new PreparedStatementBinder(conn.prepareStatement(sql, java.sql.Statement.RETURN_GENERATED_KEYS))
  }

  /**
    * Wrap given PreparedStatement in PreparedStatementBinder
    */
  def bindPs(ps: PreparedStatement): PreparedStatementBinder = {
    new PreparedStatementBinder(ps)
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
      case x if x =:= typeOf[java.time.LocalDate] => T.DATE
      case x if x =:= typeOf[java.time.LocalDateTime] => T.TIMESTAMP
      case x if x =:= typeOf[String] => T.VARCHAR
      case x if x =:= typeOf[Boolean] => T.BOOLEAN
      case x if x =:= typeOf[java.io.InputStream] => T.LONGVARBINARY

      case _ => T.JAVA_OBJECT
    }
  }

  /**
    * Convert camelCase name to snake_case name
    * @param name camelCase name
    * @return
    */
  def camelToSnake(name: String): String = {
    val accumulated = name.foldLeft((name.head, "")) {(acc, ch) =>
      if (acc._1.isLower && ch.isUpper) (ch, acc._2 + '_' +  ch.toLower)
      else (ch, acc._2 + ch.toLower)
    }
    accumulated._2
  }

  /** Convenience implicit for extending ResultSet */
  implicit class RichResultSet(private val rs: ResultSet) extends AnyVal {
    def parse[A](fn: ResultSet => A): Seq[A] = resultSetVector(rs)(fn)
    def parseOpt[A](fn: ResultSet => A): Option[A] = resultSetOpt(rs)(fn)
  }
}
