package com.hackrrank.spark.base

import com.hackrrank.spark.model.{Eligibility, Medical}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Dataset, SparkSession}

trait DataCleaningJobInterface {
  val eligibilitySchema = StructType(
    Array(
      StructField("memberId", StringType),
      StructField("firstName", StringType),
      StructField("lastName", StringType)
    )
  )

  val medicalSchema = StructType(
    Array(
      StructField("memberId", StringType),
      StructField("fullName", StringType),
      StructField("paidAmount", IntegerType)
    )
  )

  val sparkSession: SparkSession

  import sparkSession.implicits._

  def readEligibility(filePath: String): Dataset[Eligibility] = sparkSession
    .read
    .option("header", "true")
    .schema(eligibilitySchema)
    .csv(filePath)
    .as[Eligibility]

  def readMedical(filePath: String): Dataset[Medical] = sparkSession
    .read
    .option("header", "true")
    .schema(medicalSchema)
    .csv(filePath)
    .as[Medical]

  def filterMedical(eligibilityDs: Dataset[Eligibility], medicalDs: Dataset[Medical]): Dataset[Medical]

  def generateFullName(eligibilityDs: Dataset[Eligibility], medicalDs: Dataset[Medical]): Dataset[Medical]

  def findMaxPaidMember(medicalDs: Dataset[Medical]): String

  def findTotalPaidAmount(medicalDs: Dataset[Medical]): Long

  def stop(): Unit = sparkSession.stop()
}
