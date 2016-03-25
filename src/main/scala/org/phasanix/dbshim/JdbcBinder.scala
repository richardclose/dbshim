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

import java.sql.{PreparedStatement, ResultSet}
import java.sql.{Date => SqlDate}
import java.util.{Date => JUDate}
import java.time._

/**
 * Interface for creating instances from jdbc resultsets, and binding instances to prepared statements.
 * Implementations are generated by macro.
  *
  * @tparam A Bound type, either a tuple or a case class.
 * @param arity number of elements of type A
 * @param fieldNames names of elements of type A, for use in automated SQL generation
 *
 * Where methods take a parameter <code>offsets: IndexedSeq[Int]</code>, the elements of
 * <code>offsets</code> correspond to elements of A, taken in order. (So <code>offsets(0)</code>
 * corresponds to member <code>_1</code> if A is a tuple, or the first member if A is a case class,
 * and so on). The values of <code>offsets</code> are the <code>columnIndex</code>s of a
 * <code>ResultSet</code>, or the <code>parameterIndex</code>s of a <code>PreparedStatement</code>.
 */
abstract class JdbcBinder[A] (val arity: Int, val fieldNames: Seq[String]) extends Cloneable {

  protected var _zoneId: ZoneId = ZoneId.systemDefault()

  /**
    * Clone this with different zone id.
    */
  def withZoneId(zoneId: ZoneId): JdbcBinder[A] = {
    val ret = clone().asInstanceOf[JdbcBinder[A]]
    ret._zoneId = zoneId
    ret
  }

  /**
    * Time zone to use for date conversions
    * @return
    */
  def zoneId: ZoneId = _zoneId

  /**
   * Create an instance of A from the current row of the resultset.
   *
   * @param offsets map from element of A to resultset parameter index.
  */
  def fromResultSetMapped(rs: ResultSet, offsets: IndexedSeq[Int]): A

  /**
   * Create an instance of A from the current row of the ResultSet,
   * mapping the ResultSet columns directly onto the instance constructor
   */
  def fromResultSet(rs: ResultSet): A

  /**
   * Bind elements of value (in order) to the prepared statement.
   *
   * @param offsets map from element of A to parameterIndex of prepared statement.
   *                A value of 0 means "don't bind".
   */
  def bindPreparedStatement(ps: PreparedStatement, value: A, offsets: IndexedSeq[Int]): Unit

  /**
   * Bind elements of value (in order) to the prepared statement.
   */
  def bindPreparedStatement(ps: PreparedStatement, value: A): Unit

  /**
   * Bind some elements of value to the prepared statement in order.
    *
    * @param toSkip list of 0-based offsets of elements of A which will not be bound. This
   *               is used to compute offsets for the remaining elements in a call to
   *               <code>bindPreparedStatement</code>
   *               (use case: prepared statement that doesn't use all elements, e.g. with
   *               autoincrement columns)
   */
  def bindPreparedStatementPartial(ps: PreparedStatement, value: A, toSkip: Int*): Unit = {
    bindPreparedStatement(ps, value, remapFromSkipList(toSkip: _ *))
  }

  /** 
    * Bind all elements of value to the prepared statement, remapping the parameterIndex so
    * that the keys are bound at the end
    *
    * @param keyOffsets 0 based offsets of elements of A that will be moved to the end of the
   *                   parameter list.
   *                   (use case: where clause in update statement).
    */
  def bindPreparedStatementForUpdate(ps: PreparedStatement, value: A, keyOffsets: Int*): Unit = {
    bindPreparedStatement(ps, value, offsetsForUpdate(keyOffsets: _ *))
  }

  private def offsetsForUpdate(keyIndices: Int*): IndexedSeq[Int] = {
    val ret = Array.ofDim[Int](arity)
    var endPos = ret.length + 1 - keyIndices.length
    for (i <- keyIndices) {
      ret(i) = endPos
      endPos += 1
    }
    var pos = 1
    for (i <- ret.indices) {
      if (ret(i) == 0) {
        ret(i) = pos
        pos += 1
      }
    }
    ret
  }

  // Used in generated code for reading nullable columns
  protected def _opt[T](rs: ResultSet, x: T): Option[T] = if (rs.wasNull()) None else Some(x)

  // given a list of indices to skip, create a skip list
  private def remapFromSkipList(toSkip: Int*): IndexedSeq[Int] = {
    var pos = 1

    for (i <- 0 until arity) yield {
      if (toSkip.contains(i)) {
        0
      } else {
        val p = pos
        pos += 1
        p
      }
    }
  }

  protected val driverImplementsJdbc42: Boolean = false

  // Get a DATE value as a java.time.LocalDate, called from generated code.
  protected def getLocalDate(rs: ResultSet, columnIndex: Int): LocalDate = {
    var d: LocalDate = if (driverImplementsJdbc42) {
      rs.getObject(columnIndex, classOf[LocalDate])
    } else {
      null
    }
    if (d == null)
      d = rs.getDate(columnIndex).toLocalDate
    d
  }

  //  Get a TIMESTAMP value, called from generated code. Makes the assumption
  //  that the date is in the system timezone.
  protected def getLocalDateTime(rs: ResultSet, columnIndex: Int): LocalDateTime = {
    var d: LocalDateTime = if (driverImplementsJdbc42) {
      rs.getObject(columnIndex, classOf[LocalDateTime])
    } else {
      null
    }
    if (d == null) {
      val date = rs.getDate(columnIndex)
      d = ZonedDateTime.ofInstant(Instant.ofEpochMilli(date.getTime), zoneId).toLocalDateTime
    }
    d
  }

  protected def setLocalDateTime(ps: PreparedStatement, parameterIndex: Int, value: LocalDateTime): Unit = {
    val instant = ZonedDateTime.of(value, zoneId).toInstant
    val date = new SqlDate(instant.toEpochMilli)
    ps.setDate(parameterIndex, date)
  }

  protected def setLocalDate(ps: PreparedStatement, parameterIndex: Int, value: LocalDate): Unit = {
    val instant = value.atStartOfDay(zoneId).toInstant
    val date = new SqlDate(instant.toEpochMilli)
    ps.setDate(parameterIndex, date)
  }

}

object JdbcBinder {

  import reflect.macros.blackbox
  import language.experimental.macros

  /**
   * Utility methods.
   * Need to handle path-dependent types better to reduce
   * duplication.
   */
  class Helper[C <: blackbox.Context](val c: C) {

    import c.universe._
    import java.sql.{Types => T}

    def sqlTypeOf(t: Type): Int = {
      t match {
        case x if x =:= typeOf[Int] => T.INTEGER
        case x if x =:= typeOf[Long] => T.BIGINT
        case x if x =:= typeOf[Float] => T.FLOAT
        case x if x =:= typeOf[Double] => T.DOUBLE
        case x if x =:= typeOf[SqlDate] => T.DATE
        case x if x =:= typeOf[LocalDate] => T.DATE
        case x if x =:= typeOf[JUDate] => T.TIMESTAMP
        case x if x =:= typeOf[LocalDateTime] => T.TIMESTAMP
        case x if x =:= typeOf[String] => T.VARCHAR
        case x if x =:= typeOf[Boolean] => T.BOOLEAN

        case _ => T.JAVA_OBJECT
      }
    }

    def readRsExpr(t: Type, indexExpr: c.Tree): c.Tree = {
      // c.info(NoPosition, s"readRsExpr($t)", force = true)
      t match {
        case x if x =:= typeOf[Int] => q"rs.getInt($indexExpr)"
        case x if x =:= typeOf[Float] => q"rs.getFloat($indexExpr)"
        case x if x =:= typeOf[Double] => q"rs.getDouble($indexExpr)"
        case x if x =:= typeOf[String] => q"rs.getString($indexExpr)"
        case x if x =:= typeOf[JUDate] => q"rs.getDate($indexExpr)"
        case x if x =:= typeOf[LocalDate] => q"getLocalDate(rs, $indexExpr)"
        case x if x =:= typeOf[LocalDateTime] => q"getLocalDateTime(rs, $indexExpr)"
        case x if x =:= typeOf[Boolean] => q"rs.getBoolean($indexExpr)"
        case x if x =:= typeOf[Long] => q"rs.getLong($indexExpr)"
        case x if x =:= typeOf[Char] => q"rs.getString($indexExpr).headOption.getOrElse(' ')"
        case _ =>
          c.error(NoPosition, s"readRs: type not matched: $t")
          q"""Symbol("type not matched")""" // Trust that this will cause a compilation error
      }
    }

    def bindPsExpr(t: Type, propExpr: c.Tree, indexExpr: c.Tree): c.Tree = {
      // c.info(NoPosition, s"bindPsExpr($t) index: $indexExpr", force = true)
      val ex1 = t match {
        case x if x <:< typeOf[Option[_]] =>
          val typeArg = (t match {case TypeRef(_, _, args) => args}).head
          val expr = bindPsExpr(typeArg, q"x", indexExpr)
          val sqlType = sqlTypeOf(typeArg)
          q"""
             $propExpr match {
               case Some(x) => $expr
               case None => ps.setNull($indexExpr, $sqlType)
             }
           """

        case x if x =:= typeOf[Int] => q"ps.setInt($indexExpr, $propExpr)"
        case x if x =:= typeOf[Long] => q"ps.setLong($indexExpr, $propExpr)"
        case x if x =:= typeOf[Float] => q"ps.setFloat($indexExpr, $propExpr)"
        case x if x =:= typeOf[Double] => q"ps.setDouble($indexExpr, $propExpr)"
        case x if x =:= typeOf[String] => q"ps.setString($indexExpr, $propExpr)"
        case x if x =:= typeOf[JUDate] => q"ps.setDate($indexExpr, new java.sql.Date($propExpr.getTime()))"
        case x if x =:= typeOf[LocalDate] => q"setLocalDate(ps, $indexExpr, $propExpr)"
        case x if x =:= typeOf[LocalDateTime] => q"setLocalDateTime(ps, $indexExpr, $propExpr)"
        case x if x =:= typeOf[Boolean] => q"ps.setBoolean($indexExpr, $propExpr)"
        case x if x =:= typeOf[Char] => q"ps.setString($indexExpr, java.lang.String.valueOf($propExpr))"
        case _ =>
          c.error(NoPosition, s"bindPs: type not matched: $t")
          q"42"
      }

      q"if ($indexExpr != 0) {$ex1}"
    }

    /**
      * @param argTypes list of types of arguments, in order
      * @param mkIndexExpr fn giving an expression for the column index of the ResultSet
      * @return list of expressions for reading a resultset
      */
    def mkArgExprs(argTypes: List[Type], mkIndexExpr: Int => Tree): List[Tree] = {
      for ((arg, i) <- argTypes.zipWithIndex) yield {
        val indexExpr = mkIndexExpr(i)

        if (arg <:< typeOf[Option[_]]) {
          val typeArg = (arg match {
            case TypeRef(_, _, args) => args
          }).head
          val expr = readRsExpr(typeArg, indexExpr)
          q"this._opt(rs, $expr)"
        } else {
          readRsExpr(arg, indexExpr)
        }
      }
    }

  }

  /**
   * Bind the elements of a tuple to the parameters of a PreparedStatement in
   * order, in a typesafe way.
    *
    * @param ps statement to bind to
   * @param value parameter arguments
   * @tparam A Tuple of parameter arguments
   */
  def bindPreparedStatement[A <: Product](ps: PreparedStatement, value: A): Unit = macro bindPreparedStatement_impl[A]

  def bindPreparedStatement_impl[A: c.WeakTypeTag](c: blackbox.Context)(ps: c.Expr[PreparedStatement], value: c.Expr[A]) = {

    import c.universe._

    val tpe = weakTypeOf[A]
    val helper = new Helper[c.type](c)

    val bindExprs = for ((argType, i) <- tpe.typeArgs.zipWithIndex) yield {
      val index = i + 1
      val name = TermName("_" + index)
      helper.bindPsExpr(argType, q"$value.$name", q"$index")
    }

    q"{ ..$bindExprs }"
  }

  /**
   * Generate a JdbcBinder for the given type.
    *
    * @tparam A case class or tuple for which to generate a JdbcBinder
   * @return the binder
   */
  implicit def create[A]: JdbcBinder[A] = macro create_impl[A]

  def create_impl[A: c.WeakTypeTag](c: blackbox.Context) = {
    import c.universe._

    val helper = new Helper[c.type](c)
    val tpe = weakTypeOf[A]
    val isTuple = (tpe <:< typeOf[Product]) && tpe.members.exists(_.name.toString == "_1")
    val ctor = tpe.member(termNames.CONSTRUCTOR).asMethod
    val ctorParamList = ctor.paramLists.head

    val argTypes =
      if (isTuple)
        tpe.typeArgs
      else
        ctorParamList.map(_.typeSignature)

    val arity = argTypes.length

    val argExprs = helper.mkArgExprs(argTypes, { i: Int => q"offsets($i)" })
    val argExprsDirect = helper.mkArgExprs(argTypes, { i: Int => Literal(Constant(i+1)) } )

    def mkBindExprs(mkIndexExpr: Int => Tree): List[Tree] = {
      for ((sym, index) <- ctorParamList.zipWithIndex) yield {
        val nme = sym.asTerm.name
        val tpe = argTypes(index) // sym.typeSignature
        val indexExpr = mkIndexExpr(index) //
        val propExpr = q"value.$nme"

        helper.bindPsExpr(tpe, propExpr, indexExpr)
      }
    }

    val bindExprs  = mkBindExprs { i: Int => q"offsets($i)" }
    val bindExprsDirect = mkBindExprs { i: Int => Literal(Constant(i + 1)) }

    val fieldNames = ctorParamList.map(sym => sym.asTerm.name.toString)
    val fields = fieldNames.zip(argTypes).map(e => s"${e._1}: ${e._2.typeSymbol.fullName}").mkString(",")
    c.info(NoPosition, s"generated binder for ${if (isTuple) "tuple" else "case class"}: type=${tpe.typeSymbol.fullName} arity=$arity fields=$fields", force = true)

    q"""
     new org.phasanix.dbshim.JdbcBinder[$tpe]($arity, $fieldNames) {

       def fromResultSetMapped(rs: java.sql.ResultSet, offsets: IndexedSeq[Int]): $tpe = {
         return new $tpe(..$argExprs)
       }

       def fromResultSet(rs: java.sql.ResultSet): $tpe = {
         return new $tpe(..$argExprsDirect)
       }

       def bindPreparedStatement(ps: java.sql.PreparedStatement, value: $tpe, offsets: IndexedSeq[Int]): Unit = {
         ..$bindExprs
       }

       def bindPreparedStatement(ps: java.sql.PreparedStatement, value: $tpe): Unit = {
         ..$bindExprsDirect
       }

     }
   """

  }

  /**
    * Create a binder for a function returning the required type.
    * Call like this:
    * <code>
    *   val binder: JdbcBinder[MyType] = JdbcBinder.func[MyType].create(myFunction _)
    * </code>
    *
    * @tparam A type created by bound function
    */
  def func[A] = new FnBind[A]()

  class FnBind[A]() {
    def create[F](fn: F): JdbcBinder[A] = macro createFn_impl[F, A]
  }

  def createFn_impl[F: c.WeakTypeTag, A: c.WeakTypeTag](c: blackbox.Context)(fn: c.Expr[F]) = {
    import c.universe._

    val tFn = weakTypeOf[F]
    val tRet = weakTypeOf[A]
    val helper = new Helper[c.type](c)

    c.info(NoPosition, s"generating binder for function: $tFn", force = true)

    // get apply() method and type parameters, and check that the function
    // conforms.
    val apply: MethodSymbol = tFn.member(TermName("apply")).asMethod
    if (apply == NoSymbol)
      c.abort(NoPosition, s"$tFn doesn't look like a function: no apply method")
    val arity = tFn.typeArgs.length - 1
    val argTypes = tFn.typeArgs.take(arity)

    if (apply.paramLists.head.length != arity)
      c.abort(NoPosition, s"$tFn doesn't look like a Function: apply() has wrong arity")

    if (!(tRet =:= tFn.typeArgs.last))
      c.abort(NoPosition, s"$tFn doesn't return the requested type $tRet")

    val fieldNames = List.tabulate(arity) { i => s"_$i" }
    val argExprs = helper.mkArgExprs(argTypes, { i: Int => q"offsets($i)" })
    val argExprsDirect = helper.mkArgExprs(argTypes, { i: Int => Literal(Constant(i+1)) } )

    q"""
     new org.phasanix.dbshim.JdbcBinder[$tRet]($arity, $fieldNames) {
       def fromResultSetMapped(rs: java.sql.ResultSet, offsets: IndexedSeq[Int]): $tRet = {
         $fn(..$argExprs)
       }

       def fromResultSet(rs: java.sql.ResultSet): $tRet = {
         $fn(..$argExprsDirect)
       }

       def bindPreparedStatement(ps: java.sql.PreparedStatement, value: $tRet, offsets: IndexedSeq[Int]): Unit = ???

       def bindPreparedStatement(ps: java.sql.PreparedStatement, value: $tRet): Unit = ???

     }
     """
  }

}
