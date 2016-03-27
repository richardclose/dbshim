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

import java.sql.PreparedStatement
import java.util.Date

/**
 * Runtime binding -- work in progress
 */
trait BindPsParam[A] {
  def bind(ps: PreparedStatement, index: Int, value: A): Unit
}

object BindPsParam {

  import reflect.runtime.{universe => u}

  implicit val bindInt = new BindPsParam[Int] { def bind(ps: PreparedStatement, index: Int, value: Int): Unit = { ps.setInt(index, value)} }
  implicit val bindLong = new BindPsParam[Long] { def bind(ps: PreparedStatement, index: Int, value: Long): Unit = { ps.setLong(index, value)} }
  implicit val bindFloat = new BindPsParam[Float] { def bind(ps: PreparedStatement, index: Int, value: Float): Unit = { ps.setFloat(index, value)} }
  implicit val bindDouble = new BindPsParam[Double] { def bind(ps: PreparedStatement, index: Int, value: Double): Unit = { ps.setDouble(index, value)} }
  implicit val bindString = new BindPsParam[String] { def bind(ps: PreparedStatement, index: Int, value: String): Unit = { ps.setString(index, value)} }
  implicit val bindDate = new BindPsParam[Date] { def bind(ps: PreparedStatement, index: Int, value: Date): Unit = { ps.setDate(index, new java.sql.Date(value.getTime))} }

  def mkBindOpt[A : BindPsParam : u.TypeTag] = new BindPsParam[Option[A]] {
    def bind(ps: PreparedStatement, index: Int, value: Option[A]): Unit = {
      value match {
        case Some(x) => implicitly[BindPsParam[A]].bind(ps, index, x)
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
