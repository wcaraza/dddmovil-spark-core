package com.bcp.shcl.spark.core.common.util

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.util.Try
import java.io.File
import java.io.InputStreamReader

import org.springframework.security.crypto.encrypt.Encryptors
import com.typesafe.config.ConfigValueFactory
import com.typesafe.config.ConfigFactory
import com.typesafe.config.{ Config => TSConfig }

import org.apache.spark.SparkConf
import java.io.InputStream

import java.io.FileInputStream

import com.typesafe.config.impl.SimpleConfig
import com.typesafe.config.Config
import com.google.gson.JsonObject
import it.unimi.dsi.fastutil.objects.ObjectArrayList
import com.bcp.shcl
import com.bcp.shcl.spark.core.job.runner.GroupMapsideEnrichJob
import com.bcp.shcl.spark.core.job.runner.Pipeline
import com.bcp.shcl.spark.core.job.runner.MapsideEnrichJob
import com.bcp.shcl.spark.core.job.runner.EnrichJob
import com.bcp.shcl.spark.core.job.runner.Job
import com.bcp.shcl.domain.core.util.Loggable

import java.nio.file.Paths
import java.io.FileNotFoundException
import java.nio.file.Files
import com.bcp.shcl.security.CryptoManager
import com.typesafe.config.ConfigResolveOptions
import com.typesafe.config.ConfigObject
import com.typesafe.config.ConfigList
import java.util.AbstractMap
import java.util.HashMap
import com.typesafe.config.Config

/**
 * SparkConfiguration is a class wich wraps typesafe config creation and basic
 * sparkConf creation
 *
 * @author mvillabe
 * @since 08/11/2017
 */
class SparkConfiguration(sourceFile: String) extends Serializable with Loggable {

  private lazy val baseConfig: TSConfig = {
    var usedInputStream: InputStream = null
    var loadedConfig: TSConfig = null

    if (!Files.exists(Paths.get(sourceFile))) {
      logInfo(f"Config file: " + sourceFile)
      throw new FileNotFoundException(f"Config file ${sourceFile} does not exists")
    }
    ConfigFactory.parseFile(new File(sourceFile))
  }

  @transient private lazy val appConfig = decryptConfig(baseConfig)

  private def getDSEConfig(inputStream: InputStream): TSConfig = {
    val reader = new InputStreamReader(inputStream)
    val config = try {
      ConfigFactory.parseReader(reader)
    } finally {
      reader.close()
    }
    config
  }

  def getConf(): TSConfig = appConfig
  /**
   * Performs values decryption based on application.encrypted-keys list
   * parameters
   *
   * @param config Config object to be decrypted
   * @return Decrypted
   */
  private def decryptConfig(config: TSConfig): TSConfig = {
    var generatedConfig = config
    var deploymentDirectory = ""
    var dsefsDirectory = ""

    if (generatedConfig.hasPath("application.key-vault") && sys.props.contains("key-vault-app-secret")) {
      generatedConfig = generatedConfig.withValue("application.key-vault.cert-password", ConfigValueFactory.fromAnyRef(sys.props.get("key-vault-app-secret").get))
    }

    if (generatedConfig.hasPath("application.key-vault") && generatedConfig.hasPath("application.encrypted-keys")) {
      logDebug("Performing config parameters decryption")
      val cryptoList = generatedConfig.getList("application.encrypted-keys")
      val vaultConfig = generatedConfig.getConfig("application.key-vault").resolve(ConfigResolveOptions.defaults().setAllowUnresolved(true))
      val encryptor = new CryptoManager(vaultConfig)
      for (confValue <- cryptoList) {
        val decryptableKey = confValue.unwrapped().toString()
        logDebug(f"Decrypting property ${decryptableKey}")
        val currentValue = generatedConfig.getString(decryptableKey)
        generatedConfig = generatedConfig.withValue(decryptableKey, ConfigValueFactory.fromAnyRef(encryptor.decrypt(currentValue)))
      }
    }

    if (sys.props.contains("deployment-directory")) {
      deploymentDirectory = sys.props.get("deployment-directory").get
      logInfo(f"Found property deployment-directory with value ${deploymentDirectory}")
    }

    if (sys.props.contains("dsefs-directory")) {
      dsefsDirectory = sys.props.get("dsefs-directory").get
      logInfo(f"Found property dsefs-directory with value ${dsefsDirectory}")
    }

    generatedConfig = generatedConfig.withValue("application.deployment-directory", ConfigValueFactory.fromAnyRef(deploymentDirectory))
      .withValue("application.env.dsefs-directory", ConfigValueFactory.fromAnyRef(dsefsDirectory))

    generatedConfig.resolve()
  }

  /**
   * Creates the SparkConf
   *
   * @return SparkConf
   */
  def getSparkConf(): SparkConf = synchronized {
    val sparkConfig = collection.mutable.Map(appConfig.getObject("application.spark.conf").unwrapped().asScala.mapValues(_.toString).toSeq: _*)
    logDebug(f"Creating SparkConf with properties ${sparkConfig}")

    val sparkMaster = {
      val environment = sys.props.getOrElse("deployment_env", "local")
      if (environment.equalsIgnoreCase("local")) {
        logDebug("deployment_env not found using local config")
        "local[5]"
      } else {
        logDebug("Spark Master will be used in a environment different than Local, this param will be setup with DSE spark-submit")
        appConfig.getString("application.spark.master")
      }
    }

    if (appConfig.hasPath("application.credentials.azure-storage-secret") && appConfig.hasPath("application.credentials.azure-storage-key")) {
      //azure-storage-key
      val azStorageKey = appConfig.getString("application.credentials.azure-storage-key")
      val azStorageSecret = appConfig.getString("application.credentials.azure-storage-secret")
      var fsProp = f"spark.hadoop.fs.azure.account.key.${azStorageKey}.blob.core.windows.net"
      sparkConfig.put(fsProp, azStorageSecret)
    }

    new SparkConf().setAppName(appConfig.getString("application.name"))
      .setMaster(sparkMaster)
      .setAll(sparkConfig)
      .registerKryoClasses(Array(
        classOf[Pipeline],
        classOf[EnrichJob],
        classOf[MapsideEnrichJob],
        classOf[GroupMapsideEnrichJob],
        classOf[Config],
        classOf[ConfigList],
        classOf[ConfigObject],
        classOf[SparkConfiguration],
        classOf[JsonObject],
        classOf[Job]))
  }
}