package com.bcp.shcl.spark.core.common
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, DataFrameReader, DataFrameWriter, Row}
import org.apache.spark.sql.cassandra.DataFrameReaderWrapper
import com.datastax.spark.connector.CassandraRow
import com.datastax.spark.connector.toSparkContextFunctions
import java.util.Properties
import scala.collection.Map
import com.bcp.shcl.spark.core.common.util.IOUtils
import org.apache.spark.sql.types.StructType
import com.bcp.shcl.spark.core.common.util.DSEFSAuth
import java.util.HashMap
import org.apache.avro.Schema
import com.databricks.spark.avro._

/**
 * Spark Object helper used for common operations such as
 * reading from different formats(csv,avro,json,parquet,cassandra) into
 * dataframes.
 * 
 * Also provides helper methods for writing the above formats
 * 
 * @author mvillabe
 * @since 8/11/2017
 * 
 */
object SparkIO extends Serializable {
  /**
   * Empty Map used as a polifyll for options constructor on dataframe
   * read operations
   */
  @transient private lazy val emptyMap = Map[String,String]()
  
  @transient private lazy val DEFAULT_DF_MODE = "append"
  /**
   * Lazy initialized SparkSession, it will be loaded from SparkCommons
   * 
   */
  private lazy val sql = SparkCommon.sql
  /**
   * Lazy initialized SparkContext, it will be loaded from SparkCommons
   */
  private lazy val sc  = SparkCommon.sc
  
  /**
   * Dataframe read formats
   */
  def emptyDataFrame(schema:StructType):DataFrame = sql.createDataFrame(sc.emptyRDD[Row], schema)
  /**
   * Reads a parquet file from a given path, if options are specified, then it will be used to read
   * the file, returns a DataFrame based on the parquet file
   * 
   * @param path
   * @param options Read options used to read the given file
   * @return 
   */
  def readParquet(path:String,options:Map[String,String] = emptyMap):DataFrame = sql.read.options(options).parquet(IOUtils.normalizeInputPath(path))
   /**
   * Reads a csv file from a given path, if options are specified, then it will be used to read
   * the file, returns a DataFrame based on the csv file
   * 
   * @param path
   * @param options Read options used to read the given file
   * @return 
   */
  def readCSV(path:String,options:Map[String,String] = emptyMap):DataFrame = sql.read.options(options).csv(IOUtils.normalizeInputPath(path))
 /**
   * Reads an avro file from a given path, if options are specified, then it will be used to read
   * the file, returns a DataFrame based on the avro file
   * 
   * @param path
   * @param options Read options used to read the given file
   * @return 
   */
  def readAVRO(path:String,options:Map[String,String] = emptyMap):DataFrame = sql.read.options(options).avro(IOUtils.normalizeInputPath(path))
  /**
   * Reads a json file from a given path, if options are specified, then it will be used to read
   * the file, returns a DataFrame based on the json file
   * 
   * @param path
   * @param options Read options used to read the given file
   * @return 
   */
  def readJSON(path:String,options:Map[String,String] = emptyMap):DataFrame = sql.read.options(options).json(IOUtils.normalizeInputPath(path))
  /**
   * reads from cassandra tuple(keyspace,table) with given read options, returns a dataframe
   * based on the specified table
   * 
   * @param kspcTable Tuple representing(_1:keyspace, _2: table name)
   * @param options Options used to read from cassandra
   * @return 
   */
  def readCassandra(kspcTable:(String,String),options:Map[String,String] = emptyMap):DataFrame = sql.read.options(options).cassandraFormat(kspcTable._1, kspcTable._2).load()
   /**
   * reads from jdbc source returns a dataframe based on the properties
   * 
   * @param url jdbc url to be used
   * @param properties additional properties used lo load the dataframe
   * @param options Options used to read from jdbc
   * @return Dataframe based on the jdbc source configured
   */
  def readJDBC(url:String,table:String,properties:Properties,options:Map[String,String] = emptyMap):DataFrame = sql.read.options(options).jdbc(url, table, properties) 
  /**
   * Reads a parquet file from a given path, if options are specified, then it will be used to read
   * the file, returns a rdd based on the parquet file
   * 
   * @param path
   * @param options Read options used to read the given file
   * @return 
   */
  def readParquetRDD(path:String,options:Map[String,String] = emptyMap):RDD[String] = readParquet(path,options).toJSON.rdd
   /**
   * Reads a json file from a given path, if options are specified, then it will be used to read
   * the file, returns a rdd based on the json file
   * 
   * @param path
   * @param options Read options used to read the given file
   * @return 
   */
  def readJSONRDD(path:String,options:Map[String,String] = emptyMap):RDD[String] = readJSON(path,options).toJSON.rdd
 /**
   * Dataframe write formats
   */
  
  /**
   * Performs a dataframe write as a parquet file
   * 
   * @param df Dataframe to be written as parquet file
   * @param path Target path to save the file
   * @param saveMode Save mode implemented by spark any of the following:
   *   - `overwrite`: overwrite the existing data.
   *   - `append`: append the data.
   *   - `ignore`: ignore the operation (i.e. no-op).
   *   - `error`: default option, throw an exception at runtime.
   * @param options write options used to perform dataframe write
   */
  def writeParquet(df:DataFrame,path:String,saveMode:String = DEFAULT_DF_MODE,options:Map[String,String] = emptyMap):Unit = df.write.mode(saveMode).options(options).parquet(path)
    /**
   * Performs a dataframe write as a avro file
   * 
   * @param df Dataframe to be written as avro file
   * @param path Target path to save the file
   * @param saveMode Save mode implemented by spark any of the following:
   *   - `overwrite`: overwrite the existing data.
   *   - `append`: append the data.
   *   - `ignore`: ignore the operation (i.e. no-op).
   *   - `error`: default option, throw an exception at runtime.
   * @param options write options used to perform dataframe write
   */
  def writeAvro(df:DataFrame,path:String,saveMode:String = DEFAULT_DF_MODE,options:Map[String,String] = emptyMap):Unit = df.write.mode(saveMode).options(options).avro(path)
  /**m
   * Performs a dataframe write as a json file
   * 
   * @param df Dataframe to be written as json file
   * @param path Target path to save the file
   * @param saveMode Save mode implemented by spark any of the following:
   *   - `overwrite`: overwrite the existing data.
   *   - `append`: append the data.
   *   - `ignore`: ignore the operation (i.e. no-op).
   *   - `error`: default option, throw an exception at runtime.
   * @param options write options used to perform dataframe write
   */
  def writeJSON(df:DataFrame,path:String,saveMode:String = DEFAULT_DF_MODE,options:Map[String,String] = emptyMap):Unit = df.write.mode(saveMode).options(options).json(IOUtils.normalizeInputPath(path))
  /**
   * Performs a dataframe into cassandra table(must specify keyspace, and table keys on the options map)
   * 
   * @param df Dataframe to be written as cassandra table
   * @param saveMode Save mode implemented by spark any of the following:
   *   - `overwrite`: overwrite the existing data.
   *   - `append`: append the data.
   *   - `ignore`: ignore the operation (i.e. no-op).
   *   - `error`: default option, throw an exception at runtime.
   * @param options write options used to perform dataframe write
   * 
   */
  def writeCassandra(df:DataFrame,saveMode:String = DEFAULT_DF_MODE,options:Map[String,String] = emptyMap):Unit = df.write.mode(saveMode).options(options).format("org.apache.spark.sql.cassandra").save()
 /**
   * Converts a json dataframe(rdd of json serialized strings) into a dataframe
   * 
   * @param rdd RDD of json strings to be readed as dataframe
   * @param options Read options to be used
   * @return Json Dataframe
   */
  def rddToJSON(rdd:RDD[String],options:Map[String,String] = emptyMap):DataFrame  = sql.read.options(options).json(rdd)
  
  def rddToJSONSWithSchema(rdd:RDD[String],schema:StructType,options:Map[String,String] = emptyMap):DataFrame = sql.read.schema(schema).options(options).json(rdd)
  /**
   * RDD read helpers
   */
  /**
   * Read a text file as a spark RDD using a optional number of partitions
   * 
   * @param path file path to be converted into RDD
   * @param minPartitions Minimum number of partitions to be used in textFile read by default is SparkContext.defaultMinPartitions 
   * @return RDD with text file content
   */
  def readRDDText(path:String,minPartitions:Int = sc.defaultMinPartitions):RDD[String] = sc.textFile(path, minPartitions)
  /**
   * Reads a cassandra table as a RDD of cassandraRow scan
   * @param kspcTable A tuple with (_1 keyspace and _2 table name)
   * @param cql Where clause used to filter values
   * @param values Repeated values used as replacement for cql where
   * @return RDD created from the cassandra table
   */
  def readCassandraRDD(kspcTable:(String,String),cql:String, values: Any*):RDD[CassandraRow] = sc.cassandraTable(kspcTable._1, kspcTable._2).where(cql, values)
  
}