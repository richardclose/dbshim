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
   * FIXME: semantics are wrong, since hasNext is side-effecting and next is not,
   *        but maps directly onto ResultSet semantics. 
   * @param rs ResultSet to iterate
   * @param onClose function to call at the end of the ResultSet.
   */
  class CloseableResultSetIterator(rs: ResultSet, onClose: () => Unit) extends Iterator[ResultSet] {

    def hasNext: Boolean = {
      val ret = rs.next()
      if (!ret)
        close()
      ret
    }

    def next(): ResultSet = rs

    def close(): Unit = {
      rs.close()
      onClose()
    }
  }

  class Closer(toClose: AutoCloseable*) extends Function0[Unit] {
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
