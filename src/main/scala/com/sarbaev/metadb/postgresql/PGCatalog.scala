package com.sarbaev.metadb.postgresql

import com.sarbaev.metadb.utils.Sql.ResultSetIterator
import com.sarbaev.metadb.utils.Sql.RichResultSet
import java.sql.{Connection, ResultSet}

/**
 * User: yuri
 * Date: 4/6/13
 * Time: 9:38 PM 
 */
object PGCatalog {

  def executeQuery[T](query: String, mapper: ResultSet => T)(implicit connection: java.sql.Connection): Seq[T] = {
    val stmt = connection.prepareStatement(query)
    val rs = stmt.executeQuery

    rs.map(mapper).toSeq
  }

  def inList(values: Seq[Any]) = values.mkString("(\'", "\',\'", "\')") // a,b generates ('a','b')

  def typeQuery(namespaces: Seq[Int]) = s"select oid, t.* from pg_catalog.pg_type t where t.typnamespace in (${inList(namespaces)})"

  def typeMapper(set: ResultSet) =
    PGType(
      oid = set.getInt("oid"),
      typname = set.getString("typname"),
      typnamespace = set.getInt("typnamespace"),
      typlen = set.getInt("typlen"),
      typbyval = set.getBoolean("typbyval"),
      typtype = set.getString("typtype").charAt(0),
      typcategory = set.getString("typcategory").charAt(0),
      typnotnull = set.getBoolean("typnotnull")
    )

  def types(namespaces: Seq[Int])(implicit connection: java.sql.Connection): Seq[PGType] = executeQuery(typeQuery(namespaces), typeMapper(_))

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
                     relnamespace: String,
                     reltype: Int,
                     reloftype: Int,
                     relkind: Char,
                     relnatts: Int,
                     relhaspkey: Boolean,
                     relhassubclass: Boolean)

  def procQuery(namespaces: Seq[Int]) = s"""
    select oid,
      t.proargtypes::integer[] as argtypes,
      t.* from pg_catalog.pg_proc t where t.pronamespace in (${inList(namespaces)})"""

  def procMapper(set: ResultSet) = PGProc(
    oid = set.getInt("oid"),
    proname = set.getString("proname"),
    pronamespace = set.getInt("pronamespace"),
    provariadic = set.getInt("provariadic"),
    proretset = set.getBoolean("proretset"),
    pronargs = set.getInt("pronargs"),
    pronargdefaults = set.getInt("pronargdefaults"),
    prorettype = set.getInt("prorettype"),
    proargtypes = set.getIntArray("argtypes"),
    proallargtypes = set.getIntArray("proallargtypes"),
    proargmodes = set.getCharArray("proargmodes"),
    proargnames = set.getStringArray("proargnames"),
    proargdefaults = Nil//set.getString("proargdefaults")
  )

  def procs(namespaces: Seq[Int])(implicit connection: Connection): Seq[PGProc] = executeQuery(procQuery(namespaces), procMapper)
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
                    proargdefaults: Seq[String])

  def namespaceQuery(namespaces: Seq[String]) = s"select oid, t.* from pg_catalog.pg_namespace t where t.nspname in (${inList(namespaces)})"

  def namespaceMapper(set: ResultSet) = PGNamespace(set.getInt("oid"), set.getString("nspname"))

  def namespaces(namespaces: Seq[String])(implicit connection: Connection) = executeQuery(namespaceQuery(namespaces), namespaceMapper)

  case class PGNamespace(oid: Int, nspname: String)

}
