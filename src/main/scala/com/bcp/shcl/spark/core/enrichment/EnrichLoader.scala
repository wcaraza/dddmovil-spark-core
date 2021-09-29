package com.bcp.shcl.spark.core.enrichment

import scala.collection.JavaConverters._

import org.apache.spark.rdd.RDD

import com.bcp.shcl.domain.core.decoder.DecoderParser
import com.bcp.shcl.spark.core.common.SparkCommon
import com.bcp.shcl.spark.core.common.SparkIO
import com.bcp.shcl.spark.core.common.SparkIO
import com.bcp.shcl.spark.core.job.runner._
import org.apache.spark.sql.functions.trim
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.DataFrame

import com.typesafe.config.Config
import com.typesafe.config.ConfigValueFactory
import java.util.HashMap


object EnrichLoader extends Serializable {

  private def load(conf: Config): RDDDataFrame = {
    val sourceType = conf.getString("source-type")
    val format = conf.getString("data-format")
    val path = conf.getString("input-path")
    val options = if (conf.hasPath("options")) conf.getObject("options").unwrapped().asScala.mapValues(_.toString) else Map[String, String]()

    val rddDF = (sourceType, format) match {
      case ("rdd", "json") => toRDDDataFrame(SparkIO.readJSONRDD(path, options))
      case ("dataframe", "json") => dfRDDDataFrame(SparkIO.readJSON(path, options))
      case ("rdd", "parquet") => toRDDDataFrame(SparkIO.readParquetRDD(path, options))
      case ("dataframe", "parquet") => dfRDDDataFrame(SparkIO.readParquet(path, options))
      case ("dataframe", "textWithMapping") => {
        val mappingSystems = conf.getObject("mapping").unwrapped().asScala.mapValues(_.toString).asJava
        val conversion = new HashMap[String,String](mappingSystems)//Feel like this is unnecesary..
        dfRDDDataFrame(dfTextWithMapping(path, conversion ))
      }
    }
    rddDF
  }

  def dfTextWithMapping(path: String, mappingSystems: HashMap[String, String]): DataFrame = {
    val decoder = SparkCommon.sc.broadcast(new DecoderParser(mappingSystems))
    val rdd = SparkIO.readRDDText(path).mapPartitions(partition => {
      partition.map { record => decoder.value.parseRecord(record) }
    }).map { record => record.toString() }
    val df = SparkIO.rddToJSON(rdd)
    df.select(df.columns.map(value => trim(col(value)).as(value)): _*)
  }

  def load(conf: Config, sourceType: String): RDDDataFrame = {
    val addedConf = conf.withValue("source-type", ConfigValueFactory.fromAnyRef(sourceType)).resolve()
    load(addedConf)
  }
}