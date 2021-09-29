package com.bcp.shcl.spark.core.common

import java.net.URI
import java.nio.charset.Charset

import scala.io.Codec.string2codec
import scala.reflect.runtime.universe

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.array
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.udf

import com.bcp.shcl.domain.core.util.SerializeUtil
import com.bcp.shcl.spark.core.common.util.IOUtils
import com.bcp.shcl.spark.core.sql.Entity
import com.google.gson.Gson
import com.google.gson.JsonObject

import java.nio.charset.Charset
import org.apache.hadoop.io.{Text, LongWritable}
import org.apache.hadoop.mapred.TextInputFormat
import com.typesafe.config.ConfigRenderOptions

trait SHCLSparkApplication extends SparkApp {
  
  val DEFAULT_ENCODING = Charset.forName("UTF-8")


  def deserialize(x: String): JsonObject = SerializeUtil.deserialize[JsonObject](x, classOf[JsonObject])

  def asLitArray[T](xs: Seq[T]) = array(xs map lit: _*)

  def hasErrors = udf((errors: Seq[Row], requiredFields: Seq[String]) => {
    if (errors != null) {
      errors.exists(error => requiredFields.exists(_ == error.getAs[String]("error_field")))
    } else {
      false
    }
  })

  def validateArguments(args: Array[String], numberArguments: Int) {
    if (args.length != numberArguments) {
      logError(f"Unable to initialize application, it's necessary only ${numberArguments} arguments", null)
      System.exit(0)
    }
  }

  def getBatchRDD(filePath: String, encoding: String = "UTF-8"): RDD[String] = {
    val numCores = sc.getConf.get("spark.cores.max").toInt
    if (Charset.forName(encoding) == DEFAULT_ENCODING) {
      sc.textFile(filePath, numCores * 6)
    } else {
      sc.hadoopFile(filePath, classOf[TextInputFormat], classOf[LongWritable], classOf[Text],
        numCores * 6).map(pair => new String(pair._2.getBytes, 0, pair._2.getLength, encoding)).setName(filePath)
    }
  }

  def getEntity(entityPath: String): Entity = {
    val mappingFile = getConfiguration().getString(entityPath)
    val baseMapping = scala.io.Source.fromFile(IOUtils.normalizePath(mappingFile))("UTF-8").mkString
    val entity = new Gson().fromJson(baseMapping, classOf[Entity])
    entity
  }

  def getCustomFileSystem(sysProp: String): FileSystem = {
    if (sys.props.contains(sysProp)) {
      FileSystem.get(new URI(sys.props.get(sysProp).get), sc.hadoopConfiguration)
    } else {
      FileSystem.get(new Configuration())
    }
  }

}

class TestSparkConf extends SHCLSparkApplication {
   def run() {
   
    println(getConfiguration().root().render(ConfigRenderOptions.defaults().setJson(false).setOriginComments(false)))
    var a =  sc.textFile("config/base.conf")
    sc.getConf.getAll.foreach((x)=> {
      println(x._1 + " " + x._2)
    })
   }
}
object TestSparkConf {
  def main(args:Array[String]){
     sys.props.put("dsefs-directory", "dsefs://10.79.6.88:5598/shcl/bcp-shcl-transactions-batch/1.0.0-SNAPSHOT/")
     // sys.props.put("appl", value)
     var a = new TestSparkConf() 
     a.run()
  }
}