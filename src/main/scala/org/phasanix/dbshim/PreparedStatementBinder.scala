package org.phasanix.dbshim

import java.sql.PreparedStatement
import java.util.Date

/**
 */
trait PreparedStatementBinder[A] {
  def bind(ps: PreparedStatement, index: Int, value: A): Unit
}

object PreparedStatementBinder {

  import reflect.runtime.{universe => u}

  implicit val bindInt = new PreparedStatementBinder[Int] { def bind(ps: PreparedStatement, index: Int, value: Int): Unit = { ps.setInt(index, value)} }
  implicit val bindLong = new PreparedStatementBinder[Long] { def bind(ps: PreparedStatement, index: Int, value: Long): Unit = { ps.setLong(index, value)} }
  implicit val bindFloat = new PreparedStatementBinder[Float] { def bind(ps: PreparedStatement, index: Int, value: Float): Unit = { ps.setFloat(index, value)} }
  implicit val bindDouble = new PreparedStatementBinder[Double] { def bind(ps: PreparedStatement, index: Int, value: Double): Unit = { ps.setDouble(index, value)} }
  implicit val bindString = new PreparedStatementBinder[String] { def bind(ps: PreparedStatement, index: Int, value: String): Unit = { ps.setString(index, value)} }
  implicit val bindDate = new PreparedStatementBinder[Date] { def bind(ps: PreparedStatement, index: Int, value: Date): Unit = { ps.setDate(index, new java.sql.Date(value.getTime))} }

  def mkBindOpt[A : PreparedStatementBinder : u.TypeTag] = new PreparedStatementBinder[Option[A]] {
    def bind(ps: PreparedStatement, index: Int, value: Option[A]): Unit = {
      value match {
        case Some(x) => implicitly[PreparedStatementBinder[A]].bind(ps, index, x)
        case None => ps.setNull(index, Db.sqlTypeOf[A])
      }
    }
  }

  implicit val bindIntOpt = mkBindOpt[Int]
  implicit val bindLongOpt = mkBindOpt[Long]
  implicit val bindFloatOpt = mkBindOpt[Float]
  implicit val bindDoubleOpt = mkBindOpt[Double]
  implicit val bindStringOpt = mkBindOpt[String]
  implicit val bindDateOpt = mkBindOpt[Date]
}
