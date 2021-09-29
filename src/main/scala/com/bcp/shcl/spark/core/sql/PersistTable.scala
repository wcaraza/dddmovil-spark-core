package com.bcp.shcl.spark.core.sql

import com.typesafe.config.Config
import scala.collection.JavaConversions._

case class Field(name:String,alias:String)
case class Udt(name:String,_type:String)

case class PersistTable(params:Config) {
  val keyspace      = params.getString("keyspace")
  val table         = params.getString("table")
  val notNullFields = params.getString("not-null-fields").split(",").toList
  val udts          =  if(params.hasPath("udts")) params.getConfigList("udts").map { x => Udt(x.getString("name"),x.getString("type") ) }.toList else List[Udt]()
  val fields        = params.getConfigList("fields").map { x => Field(x.getString("name"),x.getString("alias") ) }.toList
}