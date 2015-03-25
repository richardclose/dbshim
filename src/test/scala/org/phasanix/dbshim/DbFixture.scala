package org.phasanix.dbshim

import java.io.{InputStreamReader, ByteArrayInputStream}
import java.sql.Connection
import javax.sql.DataSource

import org.h2.jdbcx.JdbcDataSource

/**
 * Fixture for test database.
 */
object DbFixture {

  private val dataSource: DataSource = {
    val ds = new JdbcDataSource()
    ds.setUrl("jdbc:h2:mem:test;DB_CLOSE_DELAY=-1")
    ds.setUser("sa")
    ds.setPassword("")

    val c = ds.getConnection
    init(c)
    c.close()

    ds
  }

  private def init(conn: Connection): Unit = {

    val sql =
      """drop schema if exists TEST;
        |create schema TEST;
        |set schema TEST;
        |
        |create table A (
        |  id int primary key
        |, name varchar(20) not null
        |, colour varchar(20) not null
        |, weight double null
        |);
        |
        |insert into A values (1,'apple','red',2.0),(2,'banana','yellow',null),(3,'cherry','red',1.5),(4,'damson','purple',null);
        |
      """.stripMargin

    val is = new ByteArrayInputStream(sql.getBytes)
    org.h2.tools.RunScript.execute(conn, new InputStreamReader(is))
    is.close()
  }

  /**
   * Execute the given block with a new connection, ensuring that the connection is closed
   * afterwards.
   */
  def withConnection[A](block: Connection => A): A = {
    val c = dataSource.getConnection
    try {
      block(c)
    } finally {
      c.close()
    }
  }

  /**
   * Get a new connection. It is the caller's responsibility to close it afterwards.
   */
  def getConnection: Connection = dataSource.getConnection


}
