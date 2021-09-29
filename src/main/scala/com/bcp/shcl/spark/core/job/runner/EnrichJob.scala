package com.bcp.shcl.spark.core.job.runner

import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame
import com.bcp.shcl.spark.core.enrichment.EnrichLoader
import scala.collection.JavaConverters._
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.broadcast
import com.typesafe.config.ConfigObject
import org.apache.spark.sql.functions.col
import java.util.HashMap
import scala.util.Random
import com.bcp.shcl.spark.core.common.SparkIO
import org.apache.spark.storage.StorageLevel
import com.bcp.shcl.spark.core.common.SparkIO
import com.bcp.shcl.spark.core.enrichment.EnrichLoader

/**
 * 
 */
case class EnrichField(name:String,alias:String)

class EnrichJob extends Job {
  
  @transient private lazy val joinWith:DataFrame = {
    logInfo("Invoking loadDataFrame from config " + config )
    val target = targetKeys.map(x=> col(x)).toSeq
    val columns = fields.map { x => col(x.name) }.toSeq ++ target
    EnrichLoader.load(config,"dataframe").right.get.select(columns:_*).persist(StorageLevel.MEMORY_ONLY_SER)
  }
  
  private lazy val (fields,sourceKeys,targetKeys,isBroadcast,joinType) = {
    val fields  = config.getList("join.fields").unwrapped().asScala.map {
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
    val sourceKeys  = config.getList("join.join-keys.source").unwrapped().asScala.map(_.toString)
    val targetKeys  = config.getList("join.join-keys.target").unwrapped().asScala.map(_.toString)
    val isBroadcast = if (config.hasPath("broadcast-join")) config.getBoolean("broadcast-join") else false
    val joinType    = if (config.hasPath("join.joint-type")) config.getString("join.joint-type") else "left"
      
    (fields,sourceKeys,targetKeys,isBroadcast,joinType)
  }
  
  override def setup() {
    logDebug("Printing target dataframe main schema")
    val number = joinWith.count()
    joinWith.printSchema()
  }
  
  def randomAlphaNumericString(length: Int): String = {
    val chars = ('a' to 'z') ++ ('A' to 'Z') ++ ('0' to '9')
    randomStringFromCharList(length, chars)
  }
  
  def randomStringFromCharList(length: Int, chars: Seq[Char]): String = {
    val sb = new StringBuilder
    for (i <- 1 to length) {
      val randomNum = util.Random.nextInt(chars.length)
      sb.append(chars(randomNum))
    }
    sb.toString
  }
  
  override def run(pl:Pipeline):RDDDataFrame = {
    logInfo(f"Performing join into the dataframe with join type ${joinType}  with broadcasting[${isBroadcast}]")
    val fromDataset = {
      if( pl.initDataset.isLeft ) {
         SparkIO.rddToJSON(pl.initDataset.left.get)
      } else {
        pl.initDataset.right.get
      }
    }
    
    var targetAlias = randomAlphaNumericString(4)
    var sourceAlias =  randomAlphaNumericString(4)
    val joinFields   = fields.map { x => col(f"${targetAlias}.${x.name}").alias(x.alias) }.toSeq
    val selectFields = fromDataset.columns.map { x => col(f"${sourceAlias}.${x}") }.toSeq
    val mergedFields = selectFields++joinFields
      
    val resultingDataset = {
      if (isBroadcast) {
          fromDataset.alias(sourceAlias).join(broadcast(joinWith.alias(targetAlias)),getClause(fromDataset,joinWith),joinType).select(mergedFields:_*)
      } else {
          fromDataset.alias(sourceAlias).join(joinWith.alias(targetAlias),getClause(fromDataset,joinWith),joinType).select(mergedFields:_*)
      }
    }
    dfRDDDataFrame(resultingDataset)
  }
  
  private def getClause(from:DataFrame,target:DataFrame):Column = this.synchronized {
     sourceKeys.zip(targetKeys).map{ case (c1, c2) => from(c1).eqNullSafe(target(c2)) }.reduce(_ && _)
  }
}