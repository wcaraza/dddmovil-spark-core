package com.bcp.shcl.spark.core.sql

import java.util.HashMap

import scala.collection.JavaConversions.asScalaSet
import scala.collection.JavaConversions.iterableAsScalaIterable

import org.apache.spark.annotation.InterfaceStability
import org.apache.spark.sql.Row

import com.google.gson.Gson
import com.google.gson.JsonArray
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

case class EncryptionFields(row: Row) {
  val keyvaultFields = Option(row.getAs[String]("keyvault_fields")).getOrElse("").split(",")
  val usedUdts = {
    val usedUdtsString = row.getAs[String]("used_udts")
    val usedUdtsJsomArray = new Gson().fromJson(usedUdtsString, classOf[JsonArray])
    val usedUdtsMap = new HashMap[String, Array[String]]
    if (usedUdtsJsomArray != null) {
      usedUdtsJsomArray.foreach { x =>
        x.getAsJsonObject.entrySet().foreach(x => usedUdtsMap.put(x.getKey,
          x.getValue.getAsJsonArray.map(_.getAsString).toArray[String]))
      }
    }
    usedUdtsMap
  }
  val hashFields = Option(row.getAs[String]("hash_fields")).getOrElse("").split(",")
  val unifiedKeyvaultFields = usedUdts.values().foldLeft(keyvaultFields)(_ ++ _).distinct
}



