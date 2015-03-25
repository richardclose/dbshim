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
}
