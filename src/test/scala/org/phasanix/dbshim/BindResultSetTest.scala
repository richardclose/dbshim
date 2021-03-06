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

import org.phasanix.dbshim.BindResultSetTest.factoryFn

import java.io.ByteArrayInputStream
import java.time.LocalDate
import java.util.Date
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.sql.Connection

class BindResultSetTest extends AnyFunSuite with Matchers {

  implicit val aBinder: JdbcBinder[BindResultSetTest.TestA] = JdbcBinder.create[BindResultSetTest.TestA]

  test("resultset bind should generate case classes") {
    implicit val conn: Connection = DbFixture.getConnection
    val xs = Db.autocloseQuery("select * from TEST.A order by 1")
      .map(aBinder.fromResultSet)
      .toSeq

    xs.length shouldBe 4
    xs(0).weight.isDefined shouldBe true
    xs(1).weight.isDefined shouldBe false
  }

  test("resultset bind should generate tuples") {
    implicit val conn: Connection = DbFixture.getConnection
    val tBinder = JdbcBinder.create[(Int, String, String, Option[Double])]

    val xs =  Db.autocloseQuery("select * from TEST.A order by 1")
        .map(tBinder.fromResultSet)
        .toSeq

    xs.length shouldBe 4
    xs(0)._4.isDefined shouldBe true
    xs(1)._4.isDefined shouldBe false
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

  test("binary stream insertion should work") {

    val bytes = Array.ofDim[Byte](2048)
    util.Random.nextBytes(bytes)
    val binder = JdbcBinder.create[(Long, java.io.InputStream, String)]
    val placeholders = Array.fill(binder.arity)("?")
    val key = 42L

    val toBytes = DbFixture.withConnection { implicit c =>

      val bais = new ByteArrayInputStream(bytes)

      Db.prepare(s"insert into TEST.C values(${placeholders.mkString(",")})")
        .bind(binder, (key, bais, "george"))
        .update()

      Db.withResultSet(s"select * from TEST.C where id=$key") { rs =>
        rs.next()
        val (id, stream, _) = binder.fromResultSet(rs)
        val ret = Array.ofDim[Byte](bytes.length)
        stream.read(ret)
        stream.close()
        ret
      }
    }

    bytes.corresponds(toBytes)(_ == _) shouldBe true
  }

  test("read value class should work") {
    val binder = JdbcBinder.create[BindResultSetTest.TestD]
    val d = DbFixture.withConnection { implicit c =>
      Db.withResultSetOpt(s"select id, hash from TEST.D where id=1") { rs =>
        binder.fromResultSet(rs)
      }
    }

    d.get.hash.value shouldEqual 1001L
  }

  test("write value class should work") {
    val binder = JdbcBinder.create[BindResultSetTest.TestD]
    val d = DbFixture.withConnection { implicit c =>
      Db.prepare(s"insert into TEST.D(id, hash) values(?,?)")
        .bind(binder, BindResultSetTest.TestD(10, new BindResultSetTest.Hash(3000L)))
        .update()

      Db.withResultSetOpt(s"select id, hash from TEST.D where id=10") { rs =>
        binder.fromResultSet(rs)
      }
    }

    d.get.hash.value shouldEqual 3000L
  }


  test("fluent API (parse/parseOpt) should work") {

    import Db.RichResultSet

    DbFixture.withConnection { implicit c =>
      val a = Db.prepare("select * from TEST.A order by id")
        .query()
        .parse(rs => rs.getInt(1))

      a.sum shouldEqual 10

      val b = Db.prepare("select * from TEST.A where id=?")
         .set(4)
        .parseOpt(rs => rs.getString(2))

      b.get shouldEqual "damson"
    }
  }

}

object BindResultSetTest {
  case class TestA(id: Int, name: String, colour: String, weight: Option[Double])

  case class TestB(id: Long, name: String, when: java.util.Date, theDate: java.time.LocalDate)

  class Hash(val value: Long) extends AnyVal

  case class TestD(id: Int, hash: Hash)

  def factoryFn(id: Int, name: String): BindResultSetTest.TestA = {
    BindResultSetTest.TestA(id, name, "red", None)
  }

}

