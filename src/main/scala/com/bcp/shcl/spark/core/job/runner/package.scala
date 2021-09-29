package com.bcp.shcl.spark.core.job
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame}

package object runner {
  
  type RDDDataFrame = Either[RDD[String], DataFrame]
  
  implicit def getDataFrame(rddDf:RDDDataFrame):DataFrame = rddDf.right.get
  implicit def getRDD(rddDf:RDDDataFrame):RDD[String] = rddDf.left.get
  
  implicit def toRDDDataFrame(rdd:RDD[String]):RDDDataFrame = Left(rdd)
  implicit def dfRDDDataFrame(df:DataFrame):RDDDataFrame = Right(df)
}