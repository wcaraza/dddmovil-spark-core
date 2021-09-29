package com.bcp.shcl.spark.core.job.runner

import com.typesafe.config.Config
import com.bcp.shcl.domain.core.util.Loggable

/**
 * 
 */
trait Job extends Serializable  with Loggable {
  
  private var jobCompleted: (RDDDataFrame) => Unit = (result:RDDDataFrame) => {}
  
  var config:Config = null
  
  private var setupped:Boolean = false
  
  def withConfig(config:Config):Job = {
    this.config = config
    this
  }
  
  def run(pl:Pipeline):RDDDataFrame
  
  def setup()
  
  def isSetupped():Boolean = this.setupped
  
  def setIsSetupped(setupped:Boolean) = this.setupped = setupped
  
  def apply(pipeline:Pipeline):Pipeline = {
    val pipelineResponse = this.run(pipeline)
    jobCompleted(pipelineResponse)
    pipeline.continue(pipelineResponse)
  }
  
  def onComplete(callback : (RDDDataFrame) => Unit) = this.jobCompleted = callback
}
