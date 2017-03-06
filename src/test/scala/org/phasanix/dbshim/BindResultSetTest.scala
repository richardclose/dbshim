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

import java.time.LocalDate
import java.util.Date

import org.scalatest.{FunSuite, Matchers}

class BindResultSetTest extends FunSuite with Matchers {

  implicit val aBinder = JdbcBinder.create[BindResultSetTest.TestA]

  test("resultset bind should generate case classes") {
    implicit val conn = DbFixture.getConnection
    val xs = Db.autocloseQuery("select * from TEST.A order by 1")
      .map(aBinder.fromResultSet)
      .toSeq

    xs.length shouldBe 4
    xs(0).weight shouldBe defined
    xs(1).weight shouldBe None
  }

  test("resultset bind should generate tuples") {
    implicit val conn = DbFixture.getConnection
    val tBinder = JdbcBinder.create[(Int, String, String, Option[Double])]

    val xs =  Db.autocloseQuery("select * from TEST.A order by 1")
        .map(tBinder.fromResultSet)
        .toSeq

    xs.length shouldBe 4
    xs(0)._4 shouldBe defined
    xs(1)._4 shouldBe None
  }

  test("bind tuple to PreparedStatement should work") {
    DbFixture.withConnection { implicit c =>
      val ps = c.prepareStatement("select * from TEST.A where id = ? and name = ?")
      JdbcBinder.bindPreparedStatement(ps, (2, "banana"))
      val binder = implicitly[JdbcBinder[BindResultSetTest.TestA]]
      val xs = Db.withResultSet(ps) { rs =>
        Db.asIterator(rs).map(_.getInt(1)).toIndexedSeq // must be strict, .toSeq is lazy.
      }
      xs.length shouldBe 1
      xs.head shouldBe 2
   }
  }

  test(s"binder for factory function should work (Scala version=${util.Properties.versionString})") {

    DbFixture.withConnection { implicit  c =>

      val fn =JdbcBinder.func[BindResultSetTest.TestA].bind(BindResultSetTest.factoryFn _)

      val xs = Db.autocloseQuery("select id, name from TEST.A")
        .map(fn)
        .toIndexedSeq

      xs.length shouldBe 4
      xs(3).name shouldBe "damson"
      xs(3).colour shouldBe "red"
    }
  }

  test("date conversions should work") {
    val binder: JdbcBinder[BindResultSetTest.TestB] = implicitly[JdbcBinder[BindResultSetTest.TestB]]

    val xs = DbFixture.withConnection { implicit c =>
      Db.autocloseQuery("select * from TEST.B")
        .map(binder.fromResultSet)
        .toIndexedSeq
    }

    xs.length shouldBe 3
    xs(0).toString shouldBe "TestB(1,bob,Fri Jan 01 00:00:00 GMT 2016,2014-02-02)"
    xs(1).toString shouldBe "TestB(2,fred,Sat Jan 02 00:00:00 GMT 2016,2014-03-11)"
    xs(2).toString shouldBe "TestB(3,george,Sun Jan 03 00:00:00 GMT 2016,2014-04-15)"
  }

  test ("date roundtrip should work") {
    val binder: JdbcBinder[BindResultSetTest.TestB] = implicitly[JdbcBinder[BindResultSetTest.TestB]]
    val placeholders = Array.fill(binder.arity)("?")
    val sql = s"insert into TEST.B(${binder.columnNames.mkString(",")}) values (${placeholders.mkString(",")})"

    val (before, after) = DbFixture.withConnection { implicit c =>
      val v1 = BindResultSetTest.TestB(42L, "wibble", new Date(), LocalDate.now())
      Db.prepare(sql).bind(binder, v1).update()
      val v2 = Db.withResultSet("select * from TEST.B where id = 42") { rs =>
        rs.next()
        binder.fromResultSet(rs)
      }

      (v1, v2)
    }

    before.when.getTime shouldBe after.when.getTime
  }
}

object BindResultSetTest {
  case class TestA(id: Int, name: String, colour: String, weight: Option[Double])
  case class TestB(id: Long, name: String, when: java.util.Date, theDate: java.time.LocalDate)

  def factoryFn(id: Int, name: String): BindResultSetTest.TestA = {
    BindResultSetTest.TestA(id, name, "red", None)
  }

}

