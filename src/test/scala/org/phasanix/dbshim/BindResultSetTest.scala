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
}

object BindResultSetTest {
  case class TestA(id: Int, name: String, colour: String, weight: Option[Double])
}

