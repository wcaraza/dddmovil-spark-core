package com.bcp.shcl.spark.core.job.runner

import com.typesafe.config.Config
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap
import com.bcp.shcl.spark.core.enrichment.EnrichLoader
import com.google.gson.JsonObject
import com.bcp.shcl.domain.core.util.SerializeUtil
import scala.collection.JavaConverters._
import com.bcp.shcl.spark.core.common.SparkCommon
import org.apache.spark.broadcast.Broadcast
import com.bcp.shcl.spark.core.common.SparkIO

class MapsideEnrichJob extends Job {
  
  private var sourceKeys:Broadcast[Seq[String]] = null
  
  private var targetKeys:Broadcast[Seq[String]] = null
  
  private var joinWith:Broadcast[Object2ObjectOpenHashMap[String,JsonObject]] = null
  
  override def setup() {
    val source = config.getList("join.join-keys.source").unwrapped().asScala.map { x => x.asInstanceOf[String] }.toSeq
    val target = config.getList("join.join-keys.target").unwrapped().asScala.map { x => x.asInstanceOf[String] }.toSeq
    sourceKeys = SparkCommon.sc.broadcast(source)
    targetKeys = SparkCommon.sc.broadcast(target)
    
    val localCollection = EnrichLoader.load(config,"rdd").left.get.collect().map(decodeJSON).filter{ x => qualifies(x,targetKeys.value)}.map( x => mapKeyValue(x,targetKeys.value)).toMap.asJava
  
    joinWith = SparkCommon.sc.broadcast(new Object2ObjectOpenHashMap[String,JsonObject](localCollection))
    
    logInfo(f"Loaded mapside dataset [] with total Items: ${joinWith.value.size()}")
  }
  
  @transient private lazy val fields = {
    config.getList("join.fields").unwrapped().asScala.map {
      (x) => {
         x match {
           case _:String => EnrichField(x.toString(),x.toString())
           case _ => {
             val conf = x.asInstanceOf[java.util.HashMap[String,String]]
             EnrichField(conf.get("name"),conf.get("alias"))
           }
         }
      } 
    }
  }.toSeq
  
  def qualifies(row:JsonObject,attributes:Seq[String]):Boolean = attributes.map { x => row.has(x) }.foldLeft(true)(_ && _)
  
  def mapKeyValue(row:JsonObject,attributes:Seq[String]):(String,JsonObject) = {
    val key = (attributes.map { x => {
        if(row.has(x)) {
          row.get(x).getAsString
        } else {
          logWarn(f"Mapside Row ${row.toString()} has no Attribute ${x}")
          null
        }
      } 
    }.mkString(":"),row)
    key
  }
  
  def decodeJSON(x:String):JsonObject = SerializeUtil.deserialize[JsonObject](x, classOf[JsonObject])
  
  def performBroadcastJoin(x:(String,JsonObject)):JsonObject = {
    val joinRow = joinWith.value.get(x._1)
    val resultingRow = x._2
    if( joinRow != null ) {
      fields.foreach { x => {
          if(joinRow.has(x.name)) {
            resultingRow.add(x.alias, joinRow.get(x.name))
          }
        } 
      }
    }
    resultingRow
  }

  override def run(pl:Pipeline):RDDDataFrame = {
    val fromDataset = {
      if( pl.initDataset.isLeft ) {
        pl.initDataset.left.get  
      } else {
        pl.initDataset.right.get.toJSON.rdd
      }
    }
    
    val resultingRDD = fromDataset
    .mapPartitions(partitionOfRecords => {
      partitionOfRecords.map { x =>  decodeJSON(x) }
    })
    .mapPartitions(partitionOfRecords => {
      partitionOfRecords.map { x =>  mapKeyValue(x,sourceKeys.value) }
    })
    .mapPartitions( x => {
      x.map(x => performBroadcastJoin(x))
    })
    .map { x => x.toString() }
    
    toRDDDataFrame(resultingRDD)
  }
  
}