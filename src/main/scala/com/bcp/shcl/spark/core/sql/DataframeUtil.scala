package com.bcp.shcl.spark.core.sql

import java.util.HashMap

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.JavaConversions.mapAsScalaMap
import scala.util.Try

import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.current_date
import org.apache.spark.sql.functions.date_format
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.map
import org.apache.spark.sql.functions.struct
import org.apache.spark.sql.functions.when
import org.apache.spark.sql.functions.explode

import com.bcp.shcl.spark.core.common.SparkApp
import java.util.Calendar
import com.bcp.shcl.spark.core.common.util.IOUtils
import org.apache.hadoop.fs.FileSystem
import com.typesafe.config.Config
import com.typesafe.config.ConfigRenderOptions
import com.bcp.shcl.security.CryptoManager
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.functions.udf
import com.microsoft.azure.sqldb.spark.config.{ Config => ConfigSqldb }
import com.microsoft.azure.sqldb.spark.connect._
import com.google.gson.Gson
import com.google.gson.JsonArray
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.Row

case class Entity(partitionKey: Array[String], clusteringColumns: Array[String], simpleColumns: Array[String], udtColumns: HashMap[String, Array[String]])
class ProccesCryptoManager(cryptoManagerConfigStr: String) extends Serializable {

  @transient
  private var manager: CryptoManager = null

  def getCryptoManager(): CryptoManager = {
    if (manager == null) {
      val cryptoManagerConfig = ConfigFactory.parseString(this.cryptoManagerConfigStr);
      manager = new CryptoManager(cryptoManagerConfig)
    }
    manager
  }
}

object DataframeUtil extends SparkApp {

  implicit class ExtendedDataFrame(df: DataFrame) extends Serializable {

    def groupFields(entityMapping: Seq[String], doNotDropClustering: Seq[String], doNotDropOthers: Seq[String] = Seq()): DataFrame = {
      val selectColumnsClustering = doNotDropClustering.map { column =>
        if (!hasColumn(column)) {
          lit("0").as(column)
        } else {
          col(column)
        }
      }
      val filterColumns = entityMapping.union(doNotDropOthers).filter { column => hasColumn(column) }
      val selecColumns = filterColumns.map(column => col(column))
      df.select(selecColumns.union(selectColumnsClustering): _*).na.fill("0", doNotDropClustering)
    }

    def groupFields(entity: Entity): DataFrame = {
      val selectPrimaryKey = entity.partitionKey.union(entity.clusteringColumns).map { column =>
        if (!hasColumn(column)) {
          lit("0").as(column)
        } else {
          when(col(column).isNull, "0").otherwise(col(column)).as(column)
        }
      }
      val entitySimpleColumns = if (entity.simpleColumns == null) Array[String]() else entity.simpleColumns
      val filterSimpleColumns = entitySimpleColumns.filter(column => hasColumn(column))
      val selectSimpleColumns = filterSimpleColumns.map(column => col(column))

      val entityUdtColumns = if (entity.udtColumns == null) new HashMap[String, Array[String]]() else entity.udtColumns

      val selectUdtColumns = entityUdtColumns.foldLeft(Seq[Column]())((udtColumns, udt) => {
        val attribute = udt._1
        val columns = udt._2.filter { x => hasColumn(x) }
        if (columns.size > 0) {
          udtColumns.union(Seq[Column](struct(udt._2.map { x => if (hasColumn(x)) col(x) else lit(null).as(x) }: _*).as(attribute)))
        } else {
          udtColumns
        }
      })

      df.select(selectPrimaryKey.union(selectSimpleColumns).union(selectUdtColumns): _*)
    }

    def hasColumn(path: String): Boolean = {
      Try(df(path)).isSuccess
    }

    def cassandraPersistenceError(description: String = "-"): Unit = {
      logInfo("Errors found on the application ")
      val persistTable = PersistTable(getConfiguration().getConfig("application.persistence-tables-errors"))
      val applicationName = getConfiguration().getString("application.name")
      val dfError = df.select(lit(applicationName).as("application_name"), date_format(current_date(), "yyyy-MM-dd").as("process_date"), lit(description).as("description"), col("error_id"), col("errors"), col("raw_data"))
      dfError.write
        .format("org.apache.spark.sql.cassandra")
        .mode("append")
        .options(Map("table" -> persistTable.table, "keyspace" -> persistTable.keyspace, "spark.cassandra.output.ignoreNulls" -> "true"))
        .save()
    }

    def persistCsvError(FS: FileSystem, errorPath: String, description: String = "-"): Unit = {
      logInfo("Errors found on the application ")
      val persistTable = PersistTable(getConfiguration().getConfig("application.persistence-tables-errors"))
      val applicationName = getConfiguration().getString("application.name")
      val dfError = df.select(lit(applicationName).as("application_name"), date_format(current_date(), "yyyy-MM-dd").as("process_date"), lit(description).as("description"), col("error_id"), col("errors"), col("raw_data"))
      val format = new java.text.SimpleDateFormat("yyyyMMddHHmmss")
      val now = format.format(Calendar.getInstance().getTime()).toString()
      val unMergedPath = errorPath + "/" + now + "_um.csv"
      val mergedPath = errorPath + "/" + now + ".csv"
      val header = "application_name,process_date,description,error_id,error_date,error_description,error_field,error_reason,error_stage,error_timestamp,raw_data\n"

      val dfErrorExplode = dfError.withColumn("errors", explode(dfError("errors")))
      val dfErrorDetailed = dfErrorExplode.select("application_name", "process_date", "description", "error_id", "errors.error_date", "errors.error_description", "errors.error_field", "errors.error_reason", "errors.error_stage", "errors.error_timestamp", "raw_data")

      dfErrorDetailed.write.mode("append").csv(unMergedPath)

      IOUtils.mergeFilesWithHeader(FS, unMergedPath, mergedPath, header, true)

    }

    def cassandraPersistence(persistenceTables: String = "application.persistence-tables") = {
      val persistTable = PersistTable(getConfiguration().getConfig(persistenceTables))
      df
        .write
        .format("org.apache.spark.sql.cassandra")
        .mode("append")
        .options(Map("table" -> persistTable.table, "keyspace" -> persistTable.keyspace, "spark.cassandra.output.ignoreNulls" -> "true"))
        .save()
    }

    def csvPersistenceError(FS: FileSystem, errorPath: String): Unit = {
      val unMergedPath = errorPath + "_um.csv"
      val mergedPath = errorPath + ".csv"
      val header = "application_name,process_date,description,error_id,error_date,error_description,error_field,error_reason,error_stage,error_timestamp,raw_data\n"

      val dfErrorExplode = df.withColumn("errors", explode(df("errors")))
      val dfErrorDetailed = dfErrorExplode.select("application_name", "process_date", "description", "error_id", "errors.error_date", "errors.error_description", "errors.error_field", "errors.error_reason", "errors.error_stage", "errors.error_timestamp", "raw_data")

      dfErrorDetailed.write.mode("append").csv(unMergedPath)

      IOUtils.mergeFilesWithHeader(FS, unMergedPath, mergedPath, header, true)

    }

    def TocCsvReports(FS: FileSystem, reportPath: String): Unit = {
      val unMergedPath = reportPath + "_um.csv"
      val mergedPath = reportPath + ".csv"
      val header = "\n"

      df.write.mode("overwrite").csv(unMergedPath)

      IOUtils.mergeFilesWithHeader(FS, unMergedPath, mergedPath, header, true)

    }

    def persistenceError(FS: FileSystem, errorPath: String, description: String = "-"): Unit = {
      logInfo("Errors found on the application ")
      val persistTable = PersistTable(getConfiguration().getConfig("application.persistence-tables-errors"))
      val applicationName = getConfiguration().getString("application.name")
      val dfError = df.select(lit(applicationName).as("application_name"), date_format(current_date(), "yyyy-MM-dd").as("process_date"), lit(description).as("description"), col("error_id"), col("errors"), col("raw_data"))

      dfError.cassandraPersistence("application.persistence-tables-errors")
      dfError.csvPersistenceError(FS, errorPath)

    }

    def queryEncryptFields(cryptoManagerConfig: Config, fields: List[String]): DataFrame = {
      val cryptoManagerConfigStr = cryptoManagerConfig.root().render(ConfigRenderOptions.defaults().setJson(false).setOriginComments(false))
      val proccesCryptoManager = sc.broadcast(new ProccesCryptoManager(cryptoManagerConfigStr))
      val encryptUdf = udf((value: String) => proccesCryptoManager.value.getCryptoManager().queryEncrypt(value))
      val selectColumns = df.columns.map { column =>
        if (fields.contains(column)) {
          when(col(column).isNotNull, encryptUdf(col(column)))
            .otherwise(col(column)).as(column)
        } else {
          col(column)
        }
      }
      df.select(selectColumns: _*)
    }

    def getEncryptionFields(persistTable: PersistTable): EncryptionFields = {
      val configSqlserver = collection.mutable.Map(getConfiguration().getObject("application.sql-server.conf").unwrapped().asScala.mapValues(_.toString).toSeq: _*)
      configSqlserver.put("queryCustom", f"SELECT keyvault_fields , used_udts, hash_fields FROM conf.table_alx_encryption_fields WHERE [schema] = '${persistTable.keyspace}' and [table] = '${persistTable.table}'")
      val config = ConfigSqldb(collection.immutable.Map(configSqlserver.toList: _*))
      val row = sql.sqlContext.read.sqlDB(config).first()
      EncryptionFields(row)
    }

    def encryptFlatData(persistenceTables: String): DataFrame = {
      val cryptoManagerConfigStr = getConfiguration().getConfig("application.key-vault").root().render(ConfigRenderOptions.defaults().setJson(false).setOriginComments(false))
      val proccesCryptoManager = sc.broadcast(new ProccesCryptoManager(cryptoManagerConfigStr))
      val encryptUdf = udf((value: String) => if (value == null) null else proccesCryptoManager.value.getCryptoManager().queryEncrypt(value))
      val persistTable = PersistTable(getConfiguration().getConfig(persistenceTables))
      val hash = udf((value: String) => if (value == null) null else CryptoManager.sha256(value))
      val encryptionFields = getEncryptionFields(persistTable)

      val selectColumns = df.columns.map { column =>
        if (encryptionFields.hashFields.contains(column)) {
          hash(col(column)).as(column)
        } else if (encryptionFields.unifiedKeyvaultFields.contains(column)) {
          encryptUdf(col(column)).as(column)
        } else {
          col(column)
        }
      }
      df.select(selectColumns: _*)
    }

    def decryptFlatData(persistenceTables: String): DataFrame = {
      val cryptoManagerConfigStr = getConfiguration().getConfig("application.key-vault").root().render(ConfigRenderOptions.defaults().setJson(false).setOriginComments(false))
      val proccesCryptoManager = sc.broadcast(new ProccesCryptoManager(cryptoManagerConfigStr))
      val decryptUdf = udf((value: String) => if (value == null) null else proccesCryptoManager.value.getCryptoManager().queryDecrypt(value))
      val persistTable = PersistTable(getConfiguration().getConfig(persistenceTables))
      val encryptionFields = getEncryptionFields(persistTable)

      val selectColumns = df.columns.map { column =>
        if (encryptionFields.unifiedKeyvaultFields.contains(column)) {
          decryptUdf(col(column)).as(column)
        } else {
          col(column)
        }
      }
      df.select(selectColumns: _*)
    }

    def encryptWithUdtsData(persistenceTables: String): DataFrame = {
      val cryptoManagerConfigStr = getConfiguration().getConfig("application.key-vault").root().render(ConfigRenderOptions.defaults().setJson(false).setOriginComments(false))
      val proccesCryptoManager = sc.broadcast(new ProccesCryptoManager(cryptoManagerConfigStr))
      val encryptUdf = udf((value: String) => if (value == null) null else proccesCryptoManager.value.getCryptoManager().queryEncrypt(value))
      val persistTable = PersistTable(getConfiguration().getConfig(persistenceTables))
      val hash = udf((value: String) => if (value == null) null else CryptoManager.sha256(value))
      val encryptionFields = getEncryptionFields(persistTable)

      val selectColumns = df.columns.map { column =>
        if (persistTable.udts.exists(_.name == column)) {
          val udt = persistTable.udts.find(_.name == column).get
          if (encryptionFields.usedUdts.contains(udt._type)) {
            val atributesEncrypt = encryptionFields.usedUdts.get(udt._type)
            val nestedFields = getNestedFields(column)
            struct(nestedFields.map { y =>
              if (atributesEncrypt.contains(y)) {
                encryptUdf(col(column + "." + y)).as(y)
              } else {
                col(column + "." + y).as(y)
              }
            }: _*).as(column)
          } else {
            col(column)
          }
        } else if (encryptionFields.keyvaultFields.contains(column)) {
          encryptUdf(col(column)).as(column)
        } else if (encryptionFields.hashFields.contains(column)) {
          hash(col(column)).as(column)
        } else {
          col(column)
        }
      }
      df.select(selectColumns: _*)
    }

    def decryptWithUdtsData(persistenceTables: String): DataFrame = {
      val cryptoManagerConfigStr = getConfiguration().getConfig("application.key-vault").root().render(ConfigRenderOptions.defaults().setJson(false).setOriginComments(false))
      val proccesCryptoManager = sc.broadcast(new ProccesCryptoManager(cryptoManagerConfigStr))
      val decryptUdf = udf((value: String) => if (value == null) null else proccesCryptoManager.value.getCryptoManager().queryDecrypt(value))
      val persistTable = PersistTable(getConfiguration().getConfig(persistenceTables))
      val encryptionFields = getEncryptionFields(persistTable)

      val selectColumns = df.columns.map { column =>
        if (persistTable.udts.exists(_.name == column)) {
          val udt = persistTable.udts.find(_.name == column).get
          if (encryptionFields.usedUdts.contains(udt._type)) {
            val atributesEncrypt = encryptionFields.usedUdts.get(udt._type)
            val nestedFields = getNestedFields(column)
            struct(nestedFields.map { y =>
              if (atributesEncrypt.contains(y)) {
                decryptUdf(col(column + "." + y)).as(y)
              } else {
                col(column + "." + y).as(y)
              }
            }: _*).as(column)
          } else {
            col(column)
          }
        } else if (encryptionFields.keyvaultFields.contains(column)) {
          decryptUdf(col(column)).as(column)
        } else {
          col(column)
        }
      }
      df.select(selectColumns: _*)
    }

    def getNestedFields(column: String): Seq[String] = {
      df.schema
        .filter(c => c.name == column)
        .flatMap(_.dataType.asInstanceOf[StructType].fields)
        .map(_.name)
    }

    def queryDecryptFields(cryptoManagerConfig: Config, fields: List[String]): DataFrame = {
      val cryptoManagerConfigStr = cryptoManagerConfig.root().render(ConfigRenderOptions.defaults().setJson(false).setOriginComments(false))
      val proccesCryptoManager = sc.broadcast(new ProccesCryptoManager(cryptoManagerConfigStr))
      val decryptUdf = udf((value: String) => proccesCryptoManager.value.getCryptoManager().queryDecrypt(value))
      val selectColumns = df.columns.map { column =>
        if (fields.contains(column)) {
          when(col(column).isNotNull, decryptUdf(col(column)))
            .otherwise(col(column)).as(column)
        } else {
          col(column)
        }
      }
      df.select(selectColumns: _*)
    }

  }

  def columnsToMap(columns: Seq[String]): Column = {
    map(columns.map(c => lit(c) :: col(c) :: Nil).flatten: _*)
  }
}