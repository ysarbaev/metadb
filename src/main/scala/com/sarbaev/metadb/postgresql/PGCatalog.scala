package com.sarbaev.metadb.postgresql

import com.sarbaev.metadb.utils.Sql.ResultSetIterator
import com.sarbaev.metadb.utils.Sql.RichResultSet
import java.sql.{Connection, ResultSet}
import com.sarbaev.metadb.model.Namespace

/**
 * User: yuri
 * Date: 4/6/13
 * Time: 9:38 PM 
 */
object PGCatalog {

  def executeQuery[T](query: String, mapper: RichResultSet => T)(implicit connection: java.sql.Connection): Seq[T] = {
    val stmt = connection.prepareStatement(query)
    val rs = stmt.executeQuery

    rs.map(mapper(_)).toSeq
  }

  def inList(values: Iterable[Any]) = values.mkString("(\'", "\',\'", "\')")

  // a,b generates ('a','b')

  /**
   * <pre>
   * Table "pg_catalog.pg_type"
   * typtype is b for a base type, c for a composite type (e.g., a table's row type), d for a domain, e for an enum type, p for a pseudo-type, or r for a range type. See also typrelid and typbasetype.
   * Code	Category
   * A	Array types
   * B	Boolean types
   * C	Composite types
   * D	Date/time types
   * E	Enum types
   * G	Geometric types
   * I Network address types
   * N	Numeric types
   * P	Pseudo-types
   * R	Range types
   * S	String types
   * T	Timespan types
   * U	User-defined types
   * V	Bit-string types
   * X	unknown type
   * </pre>
   */
  case class PGType(oid: Int,
                    typname: String,
                    typnamespace: Int,
                    typlen: Int,
                    typbyval: Boolean,
                    typtype: Char,
                    typcategory: Char,
                    typnotnull: Boolean)

  def typeQuery(namespaces: Iterable[Int]) = s"select oid, t.* from pg_catalog.pg_type t where t.typnamespace in (${inList(namespaces)})"

  def typeMapper(set: RichResultSet) =
    PGType(
      oid = set int "oid",
      typname = set str "typname",
      typnamespace = set int "typnamespace",
      typlen = set int "typlen",
      typbyval = set bool "typbyval",
      typtype = set char "typtype",
      typcategory = set char "typcategory",
      typnotnull = set bool "typnotnull"
    )

  def types(namespaces: Iterable[Int])(implicit connection: java.sql.Connection): Seq[PGType] = executeQuery(typeQuery(namespaces), typeMapper)


  /*
   * Table "pg_catalog.pg_tables"
   */
  case class PGTable(oid: Int, schemaname: String, tablename: String)


  /**
   * Table "pg_catalog.pg_class".
   *
   * relkind: r = ordinary table, i = index, S = sequence, v = view, c = composite type, t = TOAST table, f = foreign table
   */
  case class PGClass(oid: Int,
                     relname: String,
                     relnamespace: Int,
                     reltype: Int,
                     reloftype: Int,
                     relkind: Char,
                     relnatts: Int,
                     relhaspkey: Boolean)

  def classQuery(namespaces: Iterable[Int]) = s"select oid, t.* from pg_catalog.pg_class t where t.relnamespace in (${inList(namespaces)})"

  def classMapper(set: RichResultSet) = PGClass(
    oid = set int "oid",
    relname = set str "relname",
    relnamespace = set int "relnamespace",
    reltype = set int "reltype",
    reloftype = set int "reloftype",
    relkind = set char "relkind",
    relnatts = set int "relnatts",
    relhaspkey = set bool "relhaspkey"
  )

  def classes(namespaces: Iterable[Int])(implicit connection: Connection) = executeQuery(classQuery(namespaces), classMapper)

  /**
   * attrelid	oid	pg_class.oid	The table this column belongs to
   * attname	name	 	The column name
   * atttypid	oid	pg_type.oid	The data type of this column
   * attnum	int2	 	The number of the column. Ordinary columns are numbered from 1 up. System columns, such as oid, have (arbitrary) negative numbers.
   * attnotnull	bool	 	This represents a not-null constraint. It is possible to change this column to enable or disable the constraint.
   * atthasdef	bool	 	This column has a default value, in which case there will be a corresponding entry in the pg_attrdef catalog that actually defines the value.
   */
  case class PGAttribute(attrelid: Int,
                         attname: String,
                         atttypid: Int,
                         attnum: Int,
                         attnotnull: Boolean,
                         atthasdef: Boolean)

  def attributeQuery(namespaces: Iterable[Int]) =
    s"""
      select a.* from pg_catalog.pg_attribute a, pg_catalog.pg_class c where a.attrelid = c.oid and c.relnamespace in ( ${inList(namespaces)} )
      and a.attname not in ('tableoid', 'cmax', 'xmax', 'cmin', 'xmin', 'ctid')
    """

  def attributeMapper(set: RichResultSet) = PGAttribute(
    attrelid = set int "attrelid",
    attname = set str "attname",
    atttypid = set int "atttypid",
    attnum = set int "attnum",
    attnotnull = set bool "attnotnull",
    atthasdef = set bool "atthasdef"
  )

  def attributes(namespaces: Iterable[Int])(implicit connection: Connection) = executeQuery(attributeQuery(namespaces), attributeMapper)

  /**
   * Table "pg_catalog.pg_proc"
   */
  case class PGProc(oid: Int,
                    proname: String,
                    pronamespace: Int,
                    provariadic: Int,
                    proretset: Boolean,
                    pronargs: Int,
                    pronargdefaults: Int,
                    prorettype: Int,
                    proargtypes: Seq[Int],
                    proallargtypes: Seq[Int],
                    proargmodes: Seq[Char],
                    proargnames: Seq[String],
                    proargdefaults: Seq[PGProcDefaultParameter])

  def procQuery(namespaces: Iterable[Int]) = s"""
    select oid,
      t.proargtypes::integer[] as argtypes,
      t.* from pg_catalog.pg_proc t where t.pronamespace in (${inList(namespaces)})"""

  def procMapper(set: RichResultSet) = PGProc(
    oid = set int "oid",
    proname = set str "proname",
    pronamespace = set int "pronamespace",
    provariadic = set int "provariadic",
    proretset = set bool "proretset",
    pronargs = set int "pronargs",
    pronargdefaults = set int "pronargdefaults",
    prorettype = set int "prorettype",
    proargtypes = set.getIntArray("argtypes"),
    proallargtypes = set.getIntArray("proallargtypes"),
    proargmodes = set.getCharArray("proargmodes"),
    proargnames = set.getStringArray("proargnames"),
    proargdefaults = parseProcDefaultParameters(set str "proargdefaults")
  )

  /**
   * ({CONST :consttype 23 :consttypmod -1 :constcollid 0 :constlen 4 :constbyval true :constisnull true :location 35 :constvalue <>}
   * {CONST :consttype 23 :consttypmod -1 :constcollid 0 :constlen 4 :constbyval true :constisnull false :location 55 :constvalue 4 [ 0 1 0 0 0 0 0 0 ]})
   */
  case class PGProcDefaultParameter(consttype: Int, consttypmod: Int, constcollid: Int, constlen: Int, constbyval: Boolean, constisnull: Boolean, location: Int, constvalue: Any)

  def parseProcDefaultParameters0(params: List[String]): List[PGProcDefaultParameter] = params match {
    case Nil => Nil
    case "consttype" :: consttype :: "consttypmod" :: consttypmod :: "constcollid" :: constcollid :: "constlen" :: constlen :: "constbyval" :: constbyval :: "constisnull" :: constisnull :: "location" :: location :: "constvalue" :: constvalue :: tail =>
      PGProcDefaultParameter(
        consttype.toInt,
        consttypmod.toInt,
        constcollid.toInt,
        constlen.toInt,
        constbyval.toBoolean,
        constisnull.toBoolean,
        location.toInt,
        None
      ) :: parseProcDefaultParameters0(tail)
    case _ => Nil
  }

  def parseProcDefaultParameters(params: String): Seq[PGProcDefaultParameter] = {
    if (params == null) Nil
    else {
      val p = params.
        replaceAll("[{,},(,)]", "").
        replace("CONST", "").
        split(":").
        drop(1).
        map(_.trim).
        flatMap(_.split("\\s", 2)).
        toList

      parseProcDefaultParameters0(p)
    }

  }

  def procs(namespaces: Seq[Int])(implicit connection: Connection): Seq[PGProc] = executeQuery(procQuery(namespaces), procMapper)


  case class PGNamespace(oid: Int, nspname: String)

  def namespaceQuery(namespaces: Iterable[String]) = s"select oid, t.* from pg_catalog.pg_namespace t where t.nspname in (${inList(namespaces)})"

  def namespaceMapper(set: RichResultSet) = PGNamespace(set int ("oid"), set str ("nspname"))

  def namespaces(namespaces: Iterable[String])(implicit connection: Connection) = executeQuery(namespaceQuery(namespaces), namespaceMapper)

  def toModel(namespaces: Seq[PGNamespace], types: Seq[PGType], procs: Seq[PGProc]): Seq[Namespace] = {
    val namespaceOid = namespaces.groupBy(_.oid).map(e => e._1 -> e._2.head)
    val typesOid = types.groupBy(_.oid).map(e => e._1 -> e._2.head)
    val procsOid = procs.groupBy(_.oid).map(e => e._1 -> e._2.head)

    Nil
  }
}
