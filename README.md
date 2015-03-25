#DBShim: Typesafe Scala wrappers for JDBC

## Introduction

This library simplifies reading of Plain Old Data from `ResultSet`s, and writing 
it to `PreparedStatement`s.  If you would rather write your own SQL statements 
and use JDBC directly, but could use a bit of automation with data transfer, 
this library may be for you. I found that Anorm was very close in function 
to what I needed, but too repetitive. Slick was too rigid for my use case 
(I felt I was fighting with path-dependent types all the time), and too
much of a heavy framework.

*DBShim* works with tuples and case classes. It uses macros to generate the 
necessary calls to `ResultSet.getXxx` and `PreparedStatement.setXxx`, so it 
should to be fast, though I haven't benchmarked yet. Nullable columns are 
represented as `Option`s.

This version is a first cut -- there is a lot of work to be done on the ergonomics
of the API, so method names and signatures will change.

## Examples

    import org.phasanix.dbshim.JdbcBinder
    import JdbcBinder.create

    //
    // Corresponding table: 
    // CREATE TABLE BICYCLE(make varchar(40) not null, weight double not null, groupset varchar(40) null);
    //
    case class Bicycle (make: String, weight: Double, groupset: Option[String])
    
    def loadBikes(implicit conn: Connection): Seq[Bicycle] = {
      val stmt = conn.prepareStatement("select make, weight, groupset from BICYCLE")
      val rs = stmt.executeQuery()
      val binder = implicitly[JdbcBinder[Bicycle]]
      val arr = collection.mutable.ArrayBuffer.empty[Bicycle]
      while (rs.hasNext()) {
        arr.append(binder.fromResultSet(rs))
      }
      stmt.close()
      arr.toSeq
    }
    
    def createBike(bike: Bicycle)(implicit conn: Connection): Bicycle = {
      val stmt = conn.prepareStatement("insert into BICYCLE(make, weight, groupset) values(?,?,?)")
      val binder = implicitly[JdbcBinder[Bicycle]]
      binder.bindPreparedStatement(ps, bike)
      stmt.execute()
      stmt.close()
    }
    
    def createBike(make: String, weight: Double, groupset: Option[String])(implicit conn: Connection): Bicycle = {
      val stmt = conn.prepareStatement("insert into BICYCLE(make, weight, groupset) values(?,?,?)")
      JdbcBinder.bindPreparedStatement(ps, (make, weight, groupset))
      stmt.execute()
      stmt.close()
    }

Instances of `JdbcBinder[A]` are created a call to `JdbcBinder.create[A]`. This 
is implicit, so in the above example two instances of `JdbcBinder[Bicycle]` are
created by the calls to `implicitly`.