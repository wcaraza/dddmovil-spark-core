package com.bcp.shcl.spark.core.integration

import com.bcp.shcl.spark.core.common.SHCLSparkApplication
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import com.bcp.shcl.domain.core.rules.RuleEngine
import com.bcp.shcl.spark.core.common.util.IOUtils
import java.util.HashMap

object IntegrationSparkCoreTest extends SHCLSparkApplication {
  
 def main(args: Array[String]) {
    sc.textFile("src/test/resources/TramasVPLUSBatch.TXT", 3).foreach { x => println(x) }
 }
}