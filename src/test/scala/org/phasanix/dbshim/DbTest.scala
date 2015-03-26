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
}
