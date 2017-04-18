#DBShim: Typesafe Scala wrappers for JDBC

## Introduction

This library simplifies reading of Plain Old Data from `ResultSet`s, and writing 
it to `PreparedStatement`s.  If you would rather write your own SQL statements 
and use JDBC directly, but could use a bit of automation with data transfer, 
this library may be for you. I found that Anorm was very close in function 
to what I needed, but needed too much boilerplate. Slick was too rigid for my use case 
(I felt I was fighting with path-dependent types all the time), not great for schema-first 
design, and too much of an intrusive framework.

*DBShim* works with tuples, case classes, and types returned from method calls.
It uses macros to generate the necessary calls to `ResultSet.getXxx` and
`PreparedStatement.setXxx`, so it should to be fast, though I haven't benchmarked
yet. Nullable columns are represented as `Option`s. The datatypes supported as members
are the primitive types, JDK date types, and value classes (i.e. classes that extend 
`AnyVal`).

Instances of `JdbcBinder` are created for tuples and case classes by `JdbcBinder.create[A]`.
Create a function (`ResultSet => MyType`) for a factory method like this:
`val fn: ResultSet => MyType = JdbcBinder.func[MyType].create(makeMyType _)
`. It is necessary to convert the method to a function, as shown, so that the macro
has a type to work with.

This version is a first cut -- there is a lot of work to be done on the ergonomics
of the API, so method names and signatures will change. Java 8 is required, as conversion
to `java.time.{LocalDate,LocalDateTime}` is implemented. Also, the current scala 
macro system will be replaced by scala.meta, but `DBShim` depends on defmacros, which
aren't yet available in scala.meta.

## Examples

```scala
import org.phasanix.dbshim.JdbcBinder
import JdbcBinder.create

case class Kilos(value: Double) extends AnyVal {
  def pounds: Double = value * 2.20462
}

//
// Corresponding table: 
// CREATE TABLE BICYCLE(id: bigint identity, make varchar(40) not null, weight double not null, groupset varchar(40) null);
//
case class Bicycle (id: Long, make: String, weight: Kilos, groupset: Option[String])

object Bicycle {
  def fixie(id: Long, weight: Kilos): Bicycle = Bicycle(id, "ACME", weight, None)
}

def loadBikes(implicit conn: Connection): Seq[Bicycle] = {
  val stmt = conn.prepareStatement("select id, make, weight, groupset from BICYCLE")
  val rs = stmt.executeQuery()
  val binder = implicitly[JdbcBinder[Bicycle]]
  val arr = collection.mutable.ArrayBuffer.empty[Bicycle]
  while (rs.hasNext()) {
    arr.append(binder.fromResultSet(rs))
  }
  stmt.close()
  arr.toSeq
}

// Bind with a class
def createBike(bike: Bicycle)(implicit conn: Connection): Bicycle = {
  val stmt = conn.prepareStatement("insert into BICYCLE(make, weight, groupset) values(?,?,?)")
  val binder = implicitly[JdbcBinder[Bicycle]]
  binder.bindPreparedStatement(ps, bike)
  stmt.execute()
  stmt.close()
}

// Bind with a tuple
def createBike(make: String, weight: Double, groupset: Option[String])(implicit conn: Connection): Bicycle = {
  val stmt = conn.prepareStatement("insert into BICYCLE(make, weight, groupset) values(?,?,?)")
  JdbcBinder.bindPreparedStatement(ps, (make, weight, groupset))
  stmt.execute()
  stmt.close()
}

def loadAsFixies(implicit conn: Connection): Seq[Bicycle] = {
  val stmt = conn.prepareStatement("select id, weight from BICYCLE")
  val rs = stmt.executeQuery()
  val fn: ResultSet => Bicycle = JdbcBinder.func[Bicycle].bind(Bicycle.fixie _)
  val arr = collection.mutable.ArrayBuffer.empty[Bicycle]
  while (rs.hasNext()) {
    arr.append(fn(rs))
  }
  stmt.close()
  arr.toSeq
}

```

Instances of `JdbcBinder[A]` are created a call to `JdbcBinder.create[A]`. This 
is implicit, so in the above example two instances of `JdbcBinder[Bicycle]` are
created by the calls to `implicitly`.

## Utility methods
Although it's not the focus of this library, there are some utility methods in `Db`, for 
example an auto-closing `ResultSet` iterator. The plans are to stop short of creating
yet another comprehensive data access wrapper, so the utility methods will work directly
with JDBC classes without introducing new types or concepts.