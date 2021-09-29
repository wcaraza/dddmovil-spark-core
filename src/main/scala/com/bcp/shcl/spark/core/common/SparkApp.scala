package com.bcp.shcl.spark.core.common

import java.io.File
import scala.collection.JavaConversions._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import com.bcp.shcl.spark.core.common.util.SparkConfiguration
import com.bcp.shcl.domain.core.util.Loggable
import com.typesafe.config.Config
import org.apache.log4j.Level
import org.apache.log4j.Logger
import com.bcp.shcl.spark.core.common.util.DSEFSAuth
import java.nio.file.Paths
import java.io.FileNotFoundException
import java.nio.file.Files

/**
 * SparkCommon provides a Serializable and Loggable object used
 * to wrap the creation of the following spark app entry points:
 * <ul>
 * <li>SparkConf</li>
 * <li>SparkContext</li>
 * <li>SparkSession</li>
 * <li>StreamingContext</li>
 * </ul>
 * 
 * @author mvillabe
 * @since 08/11/2017
 */
trait SparkApp extends Serializable with Loggable {
  {
      val baseDir  = System.getProperty("user.dir")
      val log4jProps = f"${baseDir}${File.separator}config${File.separator}log4j.properties"
      if(!sys.props.contains("log4j.configuration")) {
        sys.props.put("log4j.configuration", new File(log4jProps).toURI().toString())
      }
      logInfo(f"LOG4J.props file: "+log4jProps)
      
      Logger.getLogger("io").setLevel(Level.ERROR)
      Logger.getLogger("org").setLevel(Level.ERROR)
      Logger.getLogger("akka").setLevel(Level.ERROR)
      Logger.getLogger("kafka").setLevel(Level.ERROR)
      
      if( sys.props.get("os.name").get.toLowerCase().contains("windows") ) {
        sys.props.put("hadoop.home.dir", "C:\\winutil\\")
      }
  }
  /**
   * SparkConfiguration lazy instance, it loads configuration
   * based on the current environment if no environment specified
   * then dev will be used
   */
  @transient private val sconf =  synchronized {
    val baseDir     = System.getProperty("user.dir")
    val environment = sys.props.getOrElse("application.environment", "des")
    val appType     = sys.props.getOrElse("application.config.type","config")
    var configPath  = f"${baseDir}${File.separator}config${File.separator}${environment}${File.separator}environment.conf"

    if(sys.props.contains("application.config.path")) {
      configPath = sys.props.get("application.config.path").get
    }
    
    if(!Files.exists(Paths.get(configPath))) {
        throw new FileNotFoundException(f"File ${configPath} does not exists")
    }
    
    logInfo(f"Obtained config directory ${configPath}")
    new SparkConfiguration(configPath)
  }
  /**
   * Transient lazy instance of SparkContext(loaded from configuration file controled by application.environment
   */
  @transient lazy val sc  =  SparkContext.getOrCreate(sconf.getSparkConf())

  /**
   * Transient lazy instance of SparkSession(loaded from the current sparkContext)
   */
  @transient lazy val sql = SparkSession.builder().config(sc.getConf).getOrCreate()
  /**
   * Returns configuration Object loaded in application
   * @return Application com.typesafe.config.Config config
   */
  def getConfiguration(): Config = sconf.getConf
  
}