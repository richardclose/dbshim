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

import java.sql.ResultSet

import org.scalatest.{ShouldMatchers, FunSuite}

class DbTest extends FunSuite with ShouldMatchers {

  def values(rs: ResultSet): Seq[AnyRef] = {
    (1 to rs.getMetaData.getColumnCount).map(rs.getObject)
  }

  test("resultset iterator should fetch expected values") {
    val xs = DbFixture.withConnection { implicit c =>
      val it = Db.autocloseQuery("select * from TEST.A")
      it.map(values).toIndexedSeq
    }

    xs(1)(1) shouldBe "banana"
    xs.length shouldBe 4
  }

  test("autoclose iterator should close connection") {
    implicit val conn = DbFixture.getConnection
    val it = Db.autocloseQuery("select * from TEST.A")
    val x = it.toSeq.last // read to end
    conn.isClosed shouldBe true
  }

  test("repeated calls to resultset iterator hasNext should be idempotent") {
    implicit val conn = DbFixture.getConnection
    val it = Db.autocloseQuery("select  * from TEST.A")

    // The iterator implementation should be correct in the case of
    // repeated calls to hasNext, which will result from the call to
    // isEmpty followed by reading the whole iterator.

    it.isEmpty shouldBe false
    it.isEmpty shouldBe false
    it.length shouldBe 4
  }

  test("repeated calls to resultset iterator next should not break iteration") {
    implicit val conn = DbFixture.getConnection
    val it = Db.autocloseQuery("select * from TEST.A")

    // Repeated calls to ResultSet.next() should not happen in real code
    // without interleaved calls to hasNext(), but it should still work
    // if we happen to know how many rows there are are going to be.

    (0 until 4).foreach(_ => it.next())
    it.hasNext shouldBe false
    conn.isClosed shouldBe true
  }

  test("withResultSetOpt should product Some for existing row") {

    val x: Option[(Long, String)] = DbFixture.withConnection { implicit c =>

      Db.withResultSetOpt("select * from TEST.B where id=?", 1) { rs =>
        (rs.getLong(1), rs.getString(2))
      }
    }

    x.isDefined shouldBe true
  }

  test("withResultSetOpt should product None for non-existent row") {

    val x: Option[(Long, String)] = DbFixture.withConnection { implicit c =>

      Db.withResultSetOpt("select * from TEST.B where id=?", -1) { rs =>
        (rs.getLong(1), rs.getString(2))
      }
    }

    x.isDefined shouldBe false
  }
}
