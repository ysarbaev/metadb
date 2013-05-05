package com.sarbaev.metadb

import java.sql.Types

/**
 * User: yuri
 * Date: 5/2/13
 * Time: 10:38 AM 
 */
object model {

  case class Attribute(name: Option[String], typ: Type, nullable: Boolean)
  case class Type(name: Option[String], namespace: String, sqlBaseType: Option[Int], isArray: Boolean, attributes: Seq[Attribute])

  class ParameterMode(mode: String){
    override def toString: String = mode
  }

  case object IN extends ParameterMode("IN")
  case object OUT extends ParameterMode("OUT")
  case object INOUT extends ParameterMode("INOUT")

  case class Parameter(name: Option[String], parameterType: Type, hasDefaultValue: Boolean, mode: ParameterMode)

  case class Procedure(name: String, namespace: String, parameters: Seq[Parameter], returnType: Type)

  class RelationType(rType: String){
    override def toString: String = rType
  }
  case object View extends RelationType("View")
  case object Table extends RelationType("Table")

  case class Relation(name: String, namespace: String, relationType: RelationType, typ: Type, pk: Seq[Attribute], unique: Map[String, Seq[Attribute]])

  case class Namespace(name: String, relations: Seq[Relation], procedures: Seq[Procedure])



}
