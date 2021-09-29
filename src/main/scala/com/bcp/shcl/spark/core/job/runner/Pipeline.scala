package com.bcp.shcl.spark.core.job.runner

import com.typesafe.config.Config
import scala.collection.JavaConverters._
import scala.reflect.runtime.universe
import org.apache.spark.sql.types.StructType
import com.bcp.shcl.domain.core.util.Loggable

case class Pipeline(jobList:List[Job]) extends Serializable with Loggable {
  
  var initDataset:RDDDataFrame = null
  private var defaultSchema:StructType = null
  private var taskCompleted: (RDDDataFrame) => Unit = _
  
  {
     jobList.foreach { job => { 
         if(!job.isSetupped()) {
           logInfo("Invoking job setup")
           job.setup()
           logInfo("Job Already settuped")
           job.setIsSetupped(true)
         }
       }
     }
  }
  
  def getDefaultSchema():StructType = this.defaultSchema
  
  def withInitDataset(initDataset:RDDDataFrame):Pipeline = {
    this.initDataset = initDataset 
    this
  }
  
  def withDefaultSchema(defaultSchema:StructType):Pipeline = {
    this.defaultSchema = defaultSchema
    this
  }
  
  def run(onCompleted: (RDDDataFrame) => Unit):Pipeline = {
    this.taskCompleted = onCompleted
    if (jobList.nonEmpty) jobList.head(this) else this
  }
  
  def continue(result:RDDDataFrame):Pipeline = {
   val depuredJobList = jobList.tail
   if( depuredJobList.length <= 0 ) {
     if( this.taskCompleted != null) {
       logInfo("No jobs left to invoke, invoking taskCompletedCallback")
       taskCompleted(result)
     }
   }
   Pipeline(jobList.tail).withInitDataset(result).withDefaultSchema(this.defaultSchema).run(this.taskCompleted)
  }
}

object Pipeline {
  
  def fromConfig(conf:Config,path:String):Pipeline = {
    lazy val jobList = conf.getConfigList(path).asScala.toList.filter { jobConf => {
        jobConf.hasPath("job-type")
      }
    }.map { jobConf => {
        val jobClassz = jobConf.getString("job-type")
        jobClassz match {
          case "mapside-enrich"=> new MapsideEnrichJob().withConfig(jobConf)
          case "enrich"=> new EnrichJob().withConfig(jobConf)
          case "mapside-group-enrich"=> new GroupMapsideEnrichJob().withConfig(jobConf)
          case _ => {
             val className = jobConf.getString("job-type")
             val clazz     = Class.forName(className)
             clazz.newInstance().asInstanceOf[Job].withConfig(jobConf)
          }
        }
      }
    }
    new Pipeline(jobList)
  }
}