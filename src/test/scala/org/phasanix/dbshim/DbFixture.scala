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
        |insert into A values (1,'apple','red',2.0)
        |, (2,'banana','yellow',null)
        |, (3,'cherry','red',1.5)
        |, (4,'damson','purple',null);
        |
        |create table B (
        |  id bigint primary key
        |, name varchar(20) not null
        |, when datetime not null
        |, the_date date not null
        |);
        |
        |insert into B values
        |  (1, 'bob',    '2016-01-01', '2014-02-02')
        |, (2, 'fred',   '2016-01-02', '2014-03-11')
        |, (3, 'george', '2016-01-03', '2014-04-15');
        |
        |create table C (
        |  id bigint primary key
        |, data blob not null
        |, name varchar(20) not null
        |);
        |
        |create table D (
        |  id int primary key
        |, hash bigint not null
        |);
        |
        |insert into D values
        |  (1, 1001)
        |, (2, 1002);
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
