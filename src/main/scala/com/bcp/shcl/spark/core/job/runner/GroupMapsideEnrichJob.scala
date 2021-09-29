package com.bcp.shcl.spark.core.job.runner

import com.typesafe.config.Config
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap
import com.bcp.shcl.spark.core.enrichment.EnrichLoader
import com.google.gson.JsonObject
import com.bcp.shcl.domain.core.util.SerializeUtil
import scala.collection.JavaConverters._
import scala.collection.JavaConversions._
import com.bcp.shcl.spark.core.common.SparkCommon
import org.apache.spark.broadcast.Broadcast
import com.bcp.shcl.spark.core.common.SparkIO
import it.unimi.dsi.fastutil.objects.ObjectList
import it.unimi.dsi.fastutil.objects.ObjectArrayList
import java.util.ArrayList

class GroupMapsideEnrichJob extends Job {
  /**
   * Improve performance doing some adjustments in here
   */
  private var joinWith:Broadcast[ObjectArrayList[Object2ObjectOpenHashMap[String,JsonObject]]] = null
  
  private var sourceKeys:Broadcast[List[Seq[String]]] = null
  
  private var fields:Broadcast[List[Seq[EnrichField]]] = null
 
  override def setup() {
    var localCollection = new ArrayList[Object2ObjectOpenHashMap[String,JsonObject]]
    config.getConfigList("groups").asScala.map(f => {
      val targetKey = f.getList("join.join-keys.target").unwrapped().asScala.map { x => x.asInstanceOf[String] }.toSeq
      val collected = EnrichLoader.load(f,"rdd").left.get.collect()
                                  .map(decodeJSON)
                                  .filter{ x => qualifies(x,targetKey)}
                                  .map( x => mapKeyValue(x,targetKey))
                                  .toMap
                                  .asJava
                          
      val fields = f.getList("join.fields").unwrapped().asScala.map {
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
      localCollection.add(new Object2ObjectOpenHashMap[String,JsonObject](collected))
    })
    
     var targetKeysBuffer= config.getConfigList("groups").asScala.map(f => {
       f.getList("join.fields").unwrapped().asScala.map {
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
     })
     
     var sourceKeysBuffer= config.getConfigList("groups").asScala.map(f => {
       f.getList("join.join-keys.source").unwrapped().asScala.map { x => x.asInstanceOf[String] }.toSeq
     })
      
    fields     = SparkCommon.sc.broadcast(targetKeysBuffer.toList)
    sourceKeys = SparkCommon.sc.broadcast(sourceKeysBuffer.toList)
    
    joinWith = SparkCommon.sc.broadcast(new ObjectArrayList[Object2ObjectOpenHashMap[String,JsonObject]](localCollection))
    logInfo(f"Loaded mapside dataset [] with total Items: ${joinWith.value.size()}")
  }
  
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
  
  def qualifies(row:JsonObject,attributes:Seq[String]):Boolean = attributes.map { x => row.has(x) }.foldLeft(true)(_ && _)
  
  def decodeJSON(x:String):JsonObject = SerializeUtil.deserialize[JsonObject](x, classOf[JsonObject])

  override def run(pl:Pipeline):RDDDataFrame = {
    val fromDataset = {
      if( pl.initDataset.isLeft ) {
        pl.initDataset.left.get  
      } else {
        pl.initDataset.right.get.toJSON.rdd
      }
    }
    
    val resultingRDD = fromDataset
//    .mapPartitions(partitionOfRecords => {
//      partitionOfRecords.map { x =>  decodeJSON(x) }
//    })
//    .mapPartitions( x => {
//      x.map(x => performGroupJoin(x))
//    })
    .map {x => decodeJSON(x) }
    .map {x=> performGroupJoin(x)}
    .map { x => x.toString() }
    
    toRDDDataFrame(resultingRDD)
  }
  
  def performBroadcastJoin(x:(String,JsonObject),enrichCollection:Object2ObjectOpenHashMap[String,JsonObject],fields:Seq[EnrichField]):JsonObject = {
    val joinRow = enrichCollection.get(x._1)
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
  
  def performGroupJoin(row:JsonObject):JsonObject = {
    var index = 0
    var mutatedRow = row
    for(enrichment <- joinWith.value ) {
      var keyValued = mapKeyValue(mutatedRow,sourceKeys.value(index))
      mutatedRow = performBroadcastJoin(keyValued,enrichment,fields.value(index))
      index+=1
    }
    
    mutatedRow
  }
  
}