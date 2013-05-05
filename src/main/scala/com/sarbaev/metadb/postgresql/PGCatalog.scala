package com.sarbaev.metadb.postgresql

import com.sarbaev.metadb.utils.Sql.ResultSetIterator
import java.sql.{Types, Connection}
import com.sarbaev.metadb.model._
import com.sarbaev.metadb.utils.Sql.RichResultSet
import scala.Some
import com.sarbaev.metadb.model.Namespace
import org.postgresql.core.Oid

/**
 * User: yuri
 * Date: 4/6/13
 * Time: 9:38 PM
 */
object PGCatalog {

  def inList(values: Iterable[Any]) = values.mkString("(\'", "\',\'", "\')")

  def null2option[T](v: T): Option[T] = if (v == null) None else Some(v)

  trait PGCatalogEx[T, K] {
    def query(namespaces: Iterable[K]): String

    def mapper(set: RichResultSet): T


    def exec(query: String, mapper: RichResultSet => T)(implicit connection: java.sql.Connection): Seq[T] = {
      val stmt = connection.prepareStatement(query)
      val rs = stmt.executeQuery

      rs.map(mapper(_)).toSeq
    }

    def list(namespaces: Iterable[K])(implicit connection: java.sql.Connection): Seq[T] = exec(query(namespaces), mapper)
  }

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
  case class PGType(oid          : Int,
                    typname      : String,
                    typnamespace : Int,
                    typbyval     : Boolean,
                    typtype      : Char,
                    typcategory  : Char,
                    typrelid     : Int,
                    typelem      : Int,
                    typarray     : Int,
                    typnotnull   : Boolean)

  object PGType extends PGCatalogEx[PGType, Int] {

    def query(namespaces: Iterable[Int]) = s"select oid, t.* from pg_catalog.pg_type t where t.typnamespace in (${inList(namespaces)})"

    def mapper(set: RichResultSet) =
      PGType(
        oid          = set int  "oid",
        typname      = set str  "typname",
        typnamespace = set int  "typnamespace",
        typbyval     = set bool "typbyval",
        typtype      = set char "typtype",
        typcategory  = set char "typcategory",
        typrelid     = set int  "typrelid",
        typelem      = set int  "typelem",
        typarray     = set int  "typarray",
        typnotnull   = set bool "typnotnull"
      )
  }

  /*
   * Table "pg_catalog.pg_tables"
   */
  case class PGTable(oid: Int, schemaname: String, tablename: String)


  /**
   * Table "pg_catalog.pg_class".
   *
   * relkind: r = ordinary table, i = index, S = sequence, v = view, c = composite type, t = TOAST table, f = foreign table
   */
  case class PGClass(oid          : Int,
                     relname      : String,
                     relnamespace : Int,
                     reltype      : Int,
                     reloftype    : Int,
                     relkind      : Char,
                     relnatts     : Int,
                     relhaspkey   : Boolean)


  object PGClass extends PGCatalogEx[PGClass, Int] {

    def query(namespaces: Iterable[Int]) = s"select oid, t.* from pg_catalog.pg_class t where t.relnamespace in (${inList(namespaces)})"

    def mapper(set: RichResultSet) = PGClass(
      oid          = set int  "oid",
      relname      = set str  "relname",
      relnamespace = set int  "relnamespace",
      reltype      = set int  "reltype",
      reloftype    = set int  "reloftype",
      relkind      = set char "relkind",
      relnatts     = set int  "relnatts",
      relhaspkey   = set bool "relhaspkey"
    )
  }

  /**
   * attrelid	oid	pg_class.oid	The table this column belongs to
   * attname	name	 	The column name
   * atttypid	oid	pg_type.oid	The data type of this column
   * attnum	int2	 	The number of the column. Ordinary columns are numbered from 1 up. System columns, such as oid, have (arbitrary) negative numbers.
   * attnotnull	bool	 	This represents a not-null constraint. It is possible to change this column to enable or disable the constraint.
   * atthasdef	bool	 	This column has a default value, in which case there will be a corresponding entry in the pg_attrdef catalog that actually defines the value.
   */
  case class PGAttribute(attrelid   : Int,
                         attname    : String,
                         atttypid   : Int,
                         attnum     : Int,
                         attnotnull : Boolean,
                         atthasdef  : Boolean)

  object PGAttribute extends PGCatalogEx[PGAttribute, Int] {

    def query(namespaces: Iterable[Int]) =
      s"""
      select a.* from pg_catalog.pg_attribute a, pg_catalog.pg_class c where a.attrelid = c.oid and c.relnamespace in ( ${inList(namespaces)} )
      and a.attname not in ('tableoid', 'cmax', 'xmax', 'cmin', 'xmin', 'ctid')
    """

    def mapper(set: RichResultSet) = PGAttribute(
      attrelid   = set int  "attrelid",
      attname    = set str  "attname",
      atttypid   = set int  "atttypid",
      attnum     = set int  "attnum",
      attnotnull = set bool "attnotnull",
      atthasdef  = set bool "atthasdef"
    )

  }

  case class PGConstraint(conname: String, connamespace: Int, contype: Char, conrelid: Int, conkey: Seq[Int])

  object PGConstraint extends PGCatalogEx[PGConstraint, Int] {
    def mapper(set: RichResultSet): PGConstraint = PGConstraint(
      conname      = set str    "conname",
      connamespace = set int    "connamespace",
      contype      = set char   "contype",
      conrelid     = set int    "conrelid",
      conkey       = set intArr "conkey"
    )

    def query(namespaces: Iterable[Int]): String = s"select * from pg_catalog.pg_constraint where c.connamespace in (${inList(namespaces)})"

    def primaryKeysQuery(namespaces: Iterable[Int]): String = s"select * from pg_catalog.pg_constraint where connamespace in (${inList(namespaces)}) and contype = 'p'"

    def primaryKeys(namespaces: Iterable[Int])(implicit connection: Connection): Seq[PGConstraint] = exec(primaryKeysQuery(namespaces), mapper)
  }

  /**
   * Table "pg_catalog.pg_proc"
   */
  case class PGProc(oid             : Int,
                    proname         : String,
                    pronamespace    : Int,
                    provariadic     : Int,
                    proretset       : Boolean,
                    pronargs        : Int,
                    pronargdefaults : Int,
                    prorettype      : Int,
                    proargtypes     : Seq[Int],
                    proallargtypes  : Seq[Int],
                    proargmodes     : Seq[Char],
                    proargnames     : Seq[String],
                    proargdefaults  : Seq[PGDefaultValue])

  object PGProc extends PGCatalogEx[PGProc, Int] {

    def query(namespaces: Iterable[Int]) = s"""
    select oid,
      t.proargtypes::integer[] as argtypes,
      t.* from pg_catalog.pg_proc t where t.pronamespace in (${inList(namespaces)})"""

    def mapper(set: RichResultSet) = PGProc(
      oid             = set int     "oid",
      proname         = set str     "proname",
      pronamespace    = set int     "pronamespace",
      provariadic     = set int     "provariadic",
      proretset       = set bool    "proretset",
      pronargs        = set int     "pronargs",
      pronargdefaults = set int     "pronargdefaults",
      prorettype      = set int     "prorettype",
      proargtypes     = set intArr  "argtypes",
      proallargtypes  = set intArr  "proallargtypes",
      proargmodes     = set charArr "proargmodes",
      proargnames     = set strArr  "proargnames",
      proargdefaults  = Nil
    )
  }

  case class PGAttributeDefault(adrelid: Int, adnum: Int, value: PGDefaultValue)

  object PGAttributeDefault extends PGCatalogEx[PGAttributeDefault, Int] {
    def query(namespaces: Iterable[Int]) = s"select a.* from pg_catalog.pg_attrdef a, pg_catalog.pg_class c where a.adrelid = c.oid and c.relnamespace in ( ${inList(namespaces)} )"

    def mapper(set: RichResultSet) = PGAttributeDefault(
      adrelid = set int "adrelid",
      adnum   = set int "adnum",
      value   = null //just stub
    )
  }


  /**
   * ({CONST :consttype 23 :consttypmod -1 :constcollid 0 :constlen 4 :constbyval true :constisnull true :location 35 :constvalue <>}
   * {CONST :consttype 23 :consttypmod -1 :constcollid 0 :constlen 4 :constbyval true :constisnull false :location 55 :constvalue 4 [ 0 1 0 0 0 0 0 0 ]})
   */
  case class PGDefaultValue(consttype: Int, consttypmod: Int, constcollid: Int, constlen: Int, constbyval: Boolean, constisnull: Boolean, location: Int, constvalue: Any)

  private def parseDefault0(defaultValue: String): PGDefaultValue = {
    val list = defaultValue.split(':').flatMap(_.split("\\s", 2)).map(_.trim).filter(_.length > 0).toList
    list match {
      case "consttype" :: consttype :: "consttypmod" :: consttypmod :: "constcollid" :: constcollid :: "constlen" :: constlen :: "constbyval" :: constbyval :: "constisnull" :: constisnull :: "location" :: location :: "constvalue" :: constvalue =>
        PGDefaultValue(
          consttype.toInt,
          consttypmod.toInt,
          constcollid.toInt,
          constlen.toInt,
          constbyval.toBoolean,
          constisnull.toBoolean,
          location.toInt,
          None
        )
      case _ => throw new IllegalArgumentException(s"Failed to parse : $defaultValue")
    }
  }


  def parseDefault(params: String): Seq[PGDefaultValue] = {
    val list = params.replaceAll("[{,},(,)]", "").split("CONST").map(_.trim).filter(_.length > 0)
    list.map(parseDefault0)
  }


  case class PGNamespace(oid: Int, nspname: String)

  object PGNamespace extends PGCatalogEx[PGNamespace, String] {

    def query(namespaces: Iterable[String]) = s"select oid, t.* from pg_catalog.pg_namespace t where t.nspname in (${inList(namespaces)})"

    def mapper(set: RichResultSet) = PGNamespace(set int "oid", set str "nspname")

  }

  val POSTGRESQL_TO_SQL_TYPES = Map(
    Oid.BIT         -> Types.BIT,
    Oid.BOOL        -> Types.BOOLEAN,
    Oid.BPCHAR      -> Types.VARCHAR,
    Oid.BYTEA       -> Types.BLOB,
    Oid.CHAR        -> Types.CHAR,
    Oid.DATE        -> Types.DATE,
    Oid.FLOAT4      -> Types.FLOAT,
    Oid.FLOAT8      -> Types.DOUBLE,
    Oid.INT2        -> Types.SMALLINT,
    Oid.INT4        -> Types.INTEGER,
    Oid.INT8        -> Types.BIGINT,
    Oid.INTERVAL    -> Types.OTHER,
    Oid.MONEY       -> Types.DECIMAL,
    Oid.NAME        -> Types.VARCHAR,
    Oid.NUMERIC     -> Types.NUMERIC,
    Oid.OID         -> Types.INTEGER,
    Oid.TEXT        -> Types.VARCHAR,
    Oid.TIME        -> Types.TIME,
    Oid.TIMESTAMP   -> Types.TIMESTAMP,
    Oid.TIMESTAMPTZ -> Types.TIMESTAMP,
    Oid.TIMETZ      -> Types.TIME,
    Oid.UNSPECIFIED -> Types.OTHER,
    Oid.UUID        -> Types.VARCHAR,
    Oid.VARBIT      -> Types.VARBINARY,
    Oid.VARCHAR     -> Types.VARCHAR,
    Oid.VOID        -> Types.OTHER,
    Oid.XML         -> Types.SQLXML
  )

  def toModel(namespaces  : Seq[PGNamespace],
              types       : Seq[PGType],
              classes     : Seq[PGClass],
              attributes  : Seq[PGAttribute],
              procs       : Seq[PGProc],
              constraints : Seq[PGConstraint]): Seq[Namespace] = {

    val namespacesById         = namespaces.groupBy(_.oid).map(e => e._1 -> e._2.head)
    val typesById              = types.groupBy(_.oid).map(e => e._1 -> e._2.head)
    val classesById            = classes.groupBy(_.oid).map(e => e._1 -> e._2.head)
    val attributesByClass      = attributes.groupBy(_.attrelid).map(e => e._1 -> e._2.sortBy(_.attnum))
    val classesByNamespace     = classes.groupBy(_.relnamespace)
    val constraintsByRelations = constraints.groupBy(_.conrelid)

    val result = namespaces.map {
      namespace =>

        val relations = classesByNamespace(namespace.oid) collect {

          case PGClass(relid, relname, _, reltype, reltypeof, 'r', relnatts, relhaspkey) => {

            val cons = constraintsByRelations.getOrElse(relid, Nil)

            val columns = attributesByClass(relid) map {
              attr =>

                val isPk = cons.find(_.conkey.contains(attr.attnum)).isDefined

                val nullable = !attr.attnotnull

                val pgType = typesById(attr.atttypid)

                val modelType: Type = pgType match {
                  case t@PGType(_, _, _, _, 'b', 'A', _, typelem, 0, _) => Type(None, namespace.nspname, POSTGRESQL_TO_SQL_TYPES.get(typelem), true, Seq())
//                  case t@PGType()
                }

                val typ = null; //Type()

            }

          }

          case PGClass(relid, relname, _, reltype, reltypeof, 'v', relnatts, relhaspkey) => {

          }
        }

    }


    Nil
  }
}
