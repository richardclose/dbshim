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

import java.sql.Connection

import org.scalatest.{ShouldMatchers, FunSuite}

class BindResultSetTest extends FunSuite with ShouldMatchers {

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

  test("binder for factory function should work") {

    def factoryFn(id: Int, name: String): BindResultSetTest.TestA = {
      BindResultSetTest.TestA(id, name, "red", None)
    }

    DbFixture.withConnection { implicit  c =>

      val binder: JdbcBinder[BindResultSetTest.TestA] = JdbcBinder.func[BindResultSetTest.TestA].create(factoryFn _)
      val xs = Db.autocloseQuery("select id, name from TEST.A")
        .map(binder.fromResultSet)
        .toSeq

      xs.length shouldBe 4
      xs(3).name shouldBe "damson"
      xs(3).colour shouldBe "red"
    }
  }
}

object BindResultSetTest {
  case class TestA(id: Int, name: String, colour: String, weight: Option[Double])
}

