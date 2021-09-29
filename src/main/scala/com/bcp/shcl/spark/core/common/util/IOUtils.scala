package com.bcp.shcl.spark.core.common.util

import java.io.File
import java.io.FileInputStream
import java.io.InputStream

import scala.io.Codec.string2codec
import scala.io.Source

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.FileUtil
import org.apache.hadoop.fs.Path
import com.bcp.shcl.spark.core.common.SparkCommon

import java.net.URI


case class DSEFSAuth(fsURI: String, username: String, password: String)

object IOUtils {
  /**
   *
   * Merge a group of files using hadoop utils
   * @param srcPath
   * @param dstPath
   */
  def mergeFiles(srcPath: String, dstPath: String): Unit = {
    val hadoopConfig = new Configuration()
    val hdfs = getFileSystem()
    FileUtil.copyMerge(hdfs, new Path(srcPath), hdfs, new Path(dstPath), true, hdfs.getConf, null)
  }

  def listFiles(directory: String): Array[String] = FileUtil.list(new File(directory))

  def loadDSEFile(file: String): InputStream = {
    var inputStream: InputStream = null
    if (dseEnabled()) {
      val conf = new Configuration()
      conf.set("spark.hadoop.fs.defaultFS", sys.props.getOrElse("spark.hadoop.fs.defaultFS", null));
      conf.set("spark.hadoop.fs.dsefs.impl", "com.datastax.bdp.fs.hadoop.DseFileSystem")
      conf.set("spark.hadoop.com.datastax.bdp.fs.client.authentication.factory", "com.datastax.bdp.fs.hadoop.DseRestClientAuthProviderBuilderFactory")
      conf.set("com.datastax.bdp.fs.client.authentication.basic.username", sys.props.getOrElse("spark.hadoop.com.datastax.bdp.fs.client.authentication.basic.username", null))
      conf.set("com.datastax.bdp.fs.client.authentication.basic.password", sys.props.getOrElse("spark.hadoop.com.datastax.bdp.fs.client.authentication.basic.password", null))
      val hdfs = FileSystem.get(conf)
      val path = new Path(file)
      inputStream = hdfs.open(path)
    } else {
      inputStream = new FileInputStream(normalizePath(file))
    }
    inputStream
  }

  def dseEnabled(): Boolean = {
    if (sys.props.contains("dse-enabled")) {
      var useDse = sys.props.get("dse-enabled").get.trim()
      if (useDse.equalsIgnoreCase("true")) true else false
    } else {
      false
    }

  }

  def normalizePath(path: String): String = {
    val deploymentDirectory = sys.props.getOrElse("deployment-directory", null)
    if (path.contains("file://")) {
      path.replace("file://", "")
    } else if (deploymentDirectory != null) {
      f"${deploymentDirectory}/${path}"
    } else {
      path
    }
  }
  /**
   * Normalizes dsefs path
   * @param path
   * @return
   */
  def normalizeInputPath(path: String): String = {
    val useFileSystem = IOUtils.dseEnabled()
    if (!useFileSystem) {
      path.replace("dsefs://", "file://")
    } else {
      path
    }
  }

  def getDSEFileContent(file: String): String = {
    var fileContent: String = null
    var dseFs: FileSystem = null
    var inputStream: InputStream = null
    try {
      if (dseEnabled()) {
        val conf = new Configuration()
        conf.set("fs.defaultFS", sys.props.getOrElse("spark.hadoop.fs.defaultFS", null));
        conf.set("spark.hadoop.fs.dsefs.impl", "com.datastax.bdp.fs.hadoop.DseFileSystem")
        conf.set("spark.hadoop.com.datastax.bdp.fs.client.authentication.factory", "com.datastax.bdp.fs.hadoop.DseRestClientAuthProviderBuilderFactory")
        conf.set("com.datastax.bdp.fs.client.authentication.basic.username", sys.props.getOrElse("spark.hadoop.com.datastax.bdp.fs.client.authentication.basic.username", null))
        conf.set("com.datastax.bdp.fs.client.authentication.basic.password", sys.props.getOrElse("spark.hadoop.com.datastax.bdp.fs.client.authentication.basic.password", null))

        dseFs = FileSystem.get(conf)
        val path = new Path(file)
        inputStream = dseFs.open(path)
      } else {
        inputStream = new FileInputStream(normalizePath(file))
      }
      fileContent = Source.fromInputStream(inputStream)("utf-8").mkString
    } catch {
      case e: Throwable => {
        e.printStackTrace()
      }
    } finally {
      if (dseFs != null) {
        dseFs.close()
      }
      if (inputStream != null) {
        inputStream.close()
      }
    }
    fileContent
  }
  
  def getFileSystem():FileSystem = {
    
    val hadoopConfig = new Configuration()
    var FS = FileSystem.get(hadoopConfig)
    val conf = SparkCommon.getConfiguration()
    
    if(conf.hasPath(f"application.repository.blob-storage")){
      val path = conf.getString(f"application.repository.blob-storage.root")    
      var FS = FileSystem.get(new URI(path), SparkCommon.sc.hadoopConfiguration)
    }
    FS
  }
  
  def mergeFilesWithHeader(FS:FileSystem , srcPath: String, dstPath: String, header: String, deleteSource: Boolean): Unit = {
    
    val srcDir = new Path(srcPath)
    val dstFile = new Path(dstPath)
    
    val out = FS.create(dstFile)
    out.write(header.getBytes("UTF-8"))
    val contents: Array[FileStatus] = FS.listStatus(srcDir)
    for (i <- 0 until contents.length by 1) {
      if (contents(i).isFile() && contents(i).getPath().getName().startsWith("part-")) {
        var fileName = contents(i).getPath().getName()
        var in = FS.open(contents(i).getPath())
        org.apache.hadoop.io.IOUtils.copyBytes(in, out, FS.getConf, false)
        in.close()
      }
    }
    out.close()

    if (deleteSource) {
      FS.delete(srcDir, true)
    }

  }

}