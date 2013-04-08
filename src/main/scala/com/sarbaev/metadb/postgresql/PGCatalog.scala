package com.sarbaev.metadb.postgresql

import com.sarbaev.metadb.utils.Sql.ResultSetIterator

/**
 * User: yuri
 * Date: 4/6/13
 * Time: 9:38 PM 
 */
object PGCatalog {

  def pgTypeQuery(namespaces: Seq[Int]) = s"select oid, t.* from pg_catalog.pg_type t where t.typnamespace in (${namespaces.mkString(",")})"

  def pgTypes(namespaces: Seq[Int])(implicit connection: java.sql.Connection): Seq[PGType] = {
    val stmt = connection.prepareStatement(pgTypeQuery(namespaces))

    val rs = stmt.executeQuery()

    val types = rs.map {
      r => PGType(
        oid = r.getInt("oid"),
        typname = r.getString("typname"),
        typnamespace = r.getInt("typnamespace"),
        typlen = r.getInt("typlen"),
        typbyval = r.getInt("typbyval"),
        typtype = r.getString("typtype").charAt(0),
        typcategory = r.getString("typcategory").charAt(0),
        typnotnull = r.getBoolean("typnotnull")
      )
    }

    types.toSeq
  }

  /**
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
   */
  case class PGType(oid: Int,
                    typname: String,
                    typnamespace: Int,
                    typlen: Int,
                    typbyval: Int,
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

  /**
   * Table "pg_catalog.pg_proc"
   */
  case class PGProc(oid: Int,
                    proname: String,
                    pronamespace: String,
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

  def pgNamespaceQuery(namespaces: Seq[String]) = s"select oid, t.* from pg_catalog.pg_namespace where t.nspname in (${namespaces.map('\'' + _ + '\'').mkString(",")})"

  case class PGNamespace(oid: Int, nspname: String)

}
