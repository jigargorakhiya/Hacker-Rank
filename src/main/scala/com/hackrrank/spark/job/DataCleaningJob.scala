package com.hackrrank.spark.job

import com.hackrrank.spark.base.DataCleaningJobInterface
import com.hackrrank.spark.model.{Eligibility, Medical}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.{lit, _}

object DataCleaningJob extends DataCleaningJobInterface {

  val sparkSession: SparkSession = SparkSession.builder()
    .master("local")
    .appName("Data Cleaning")
    .getOrCreate()

  import sparkSession.implicits._

  override def readEligibility(filePath: String): Dataset[Eligibility] = sparkSession
    .read
    .option("header", "true")
    .schema(eligibilitySchema)
    .csv(filePath)
    .as[Eligibility]

  override def readMedical(filePath: String): Dataset[Medical] = sparkSession
    .read
    .option("header", "true")
    .schema(medicalSchema)
    .csv(filePath)
    .as[Medical]

  override def filterMedical(eligibilityDs: Dataset[Eligibility], medicalDs: Dataset[Medical]): Dataset[Medical] = {

    val filterMedicalDs = eligibilityDs.join(medicalDs,eligibilityDs("memberId") === medicalDs("memberId"),"left")

    filterMedicalDs.select(medicalDs("memberId"),medicalDs("fullName"),medicalDs("paidAmount")).as[Medical]

  }

  override def generateFullName(eligibilityDs: Dataset[Eligibility], medicalDs: Dataset[Medical]): Dataset[Medical] = {

    val filterMedicalDs = eligibilityDs.join(medicalDs,eligibilityDs("memberId") === medicalDs("memberId"),"inner")

    filterMedicalDs.select(medicalDs("memberId"),
      concat(eligibilityDs("firstName"),lit(" "),eligibilityDs("lastName")).as("fullName"),medicalDs("paidAmount")
    ).as[Medical]
  }

  override def findMaxPaidMember(medicalDs: Dataset[Medical]): String = {

    val maxPaidAmount = medicalDs.agg(max("paidAmount").cast("int")).first.getInt(0)

    medicalDs.select("memberId").where( "paidAmount = "+ maxPaidAmount).first().getString(0)

  }

  override def findTotalPaidAmount(medicalDs: Dataset[Medical]): Long = {
    medicalDs.agg(sum("paidAmount").cast("long")).first.getLong(0)
  }
}
